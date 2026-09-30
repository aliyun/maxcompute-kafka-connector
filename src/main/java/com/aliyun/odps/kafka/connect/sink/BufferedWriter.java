package com.aliyun.odps.kafka.connect.sink;

import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.TimeZone;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.kafka.connect.ConfigParameter;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.kafka.connect.ConnectorConfig;
import com.aliyun.odps.kafka.connect.PartitionWindowType;
import com.aliyun.odps.kafka.connect.SinkStatusContext.Status;
import com.aliyun.odps.kafka.connect.converter.RecordConverter;
import com.aliyun.odps.kafka.connect.utils.OdpsUtils;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aliyun.odps.kafka.connect.ConfigParameter.BUFFER_SIZE_KB;
import static com.aliyun.odps.kafka.connect.ConfigParameter.FAIL_RETRY_TIMES;
import static com.aliyun.odps.kafka.connect.ConfigParameter.PARTITION_WINDOW_TYPE;
import static com.aliyun.odps.kafka.connect.ConfigParameter.SKIP_ERROR;
import static com.aliyun.odps.kafka.connect.ConfigParameter.TIME_ZONE;
import static com.aliyun.odps.kafka.connect.ConfigParameter.USE_NEW_PARTITION_FORMAT;

public class BufferedWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferedWriter.class);
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss");
    private static final DateTimeFormatter DAY_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH");
    private static final DateTimeFormatter MINUTE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    private static final int DEFAULT_RETRY_TIMES = 3;
    private static final int DEFAULT_RETRY_INTERVAL_SECONDS = 10;

    /*
      Configs of this sink writer, won't change
     */
    private final TableTunnel tunnel;
    private final String project;
    private final String table;
    private final RecordConverter converter;
    private final ErrantRecordReporter errorReporter;
    private final PartitionWindowType partitionWindowType;
    private final TimeZone tz;
    private final boolean useNewPartitionFormat;
    private final boolean skipError;
    private final int bufferLimitBytes;
    private final int retryTimes;

    /*
      Internal states of this sink writer, could change
     */
    private TableTunnel.StreamUploadSession streamSession;
    private TableTunnel.StreamRecordPack streamPack;
    private Record reusedRecord;

    private Long partitionStartTime;
    private long batchInsertTime = -1;
    private long processedRecords = 0;
    private long startOffset = -1;
    private long maxOffset = -1;
    /**
     * 本缓冲窗口内没有落盘、也没有可上报去向的记录条数。它们同样会把 maxOffset 往前推，
     * 所以必须单独记账，交给提交位点那一步显式说明——否则水位越过的就是一条查无此据的记录。
     */
    private long droppedRecords = 0;
    /** 本缓冲窗口内转换/追加失败但已交给错误上报通道的记录条数（有去向，不等于已投递成功）。 */
    private long reportedRecords = 0;

    public BufferedWriter(Odps odps, ConnectorConfig config, String project, String table, RecordConverter converter,
        ErrantRecordReporter errorReporter) {
        this(odps, config, project, table, converter, errorReporter, OdpsUtils.getTableTunnel(odps, config));
    }

    /**
     * 注入 tunnel 的构造：让"坏记录怎么处理"这条策略可以在不连 MaxCompute 的情况下被逐条驱动和断言
     * （{@code StreamUploadSession} / {@code StreamRecordPack} 在 SDK 里都是接口）。
     * 生产路径请使用上一个构造函数。
     */
    BufferedWriter(Odps odps, ConnectorConfig config, String project, String table, RecordConverter converter,
        ErrantRecordReporter errorReporter, TableTunnel tunnel) {

        this.tunnel = Objects.requireNonNull(tunnel);
        this.project = Objects.requireNonNull(project);
        this.table = Objects.requireNonNull(table);
        this.converter = Objects.requireNonNull(converter);
        this.bufferLimitBytes = 1024 * BUFFER_SIZE_KB.getInt(config);
        this.partitionWindowType = PartitionWindowType.valueOf(PARTITION_WINDOW_TYPE.getString(config));
        this.useNewPartitionFormat = USE_NEW_PARTITION_FORMAT.getBoolean(config);
        this.tz = Objects.requireNonNull(TimeZone.getTimeZone(TIME_ZONE.getString(config)));
        int timesInt = FAIL_RETRY_TIMES.getInt(config);
        this.retryTimes = timesInt < 0 ? DEFAULT_RETRY_TIMES : timesInt;
        this.skipError = SKIP_ERROR.getBoolean(config);
        this.errorReporter = errorReporter;
        reset();
    }

    public synchronized boolean write(SinkRecord record) {
        // first record
        if (batchInsertTime == -1) {
            startOffset = record.kafkaOffset();
            batchInsertTime = Instant.ofEpochMilli(System.currentTimeMillis()).atZone(tz.toZoneId()).toEpochSecond();
            try {
                initStreamUploadSession(batchInsertTime);
            } catch (Throwable e) {
                LOGGER.error("resetStreamUploadSession failed", e);
                throw new RuntimeException(e);
            }
        }
        try {
            // reusedRecord 整个窗口只创建一次并被逐条复用：上一条写过、这一条没写的列会原样留着，
            // 于是"值为 NULL 的墓碑记录"或"JSON 里缺字段"会落出一条带着别的记录内容的水印行。
            // 转换器契约（RecordConverter#convert 的注释：reuse this object）要求逐条自足，这里在调用前清空。
            clearRecord(reusedRecord);
            converter.convert(record, reusedRecord);
            streamPack.append(reusedRecord);
            processedRecords++;
        } catch (Throwable e) {
            handleFailedRecord(record, e);
        }
        maxOffset = Math.max(maxOffset, record.kafkaOffset());
        return streamPack.getDataSize() >= bufferLimitBytes;
    }

    /**
     * 一条没落盘的记录只有三种去向：上报（有去处）、抛错（让 worker 决定重试或失败）、被跳过。
     * 只有"被跳过"是既没落盘也没去处的，它必须留下带 offset 的日志并计入 droppedRecords；
     * 原实现在 errorReporter == null 且 skip_error=true 时连一行日志都没有，水位却照样前进。
     *
     * <p>上报通道自身抛错（例如 DLQ 生产者不可用）不等于这条记录有了去处：按同样的策略重走一遍，
     * 即未开启容错时抛出去，开启容错时按丢弃记账。
     */
    private void handleFailedRecord(SinkRecord record, Throwable cause) {
        if (errorReporter != null) {
            try {
                errorReporter.report(record, cause);
                reportedRecords++;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Reported record {}-{}@{} to the error topic instead of writing it",
                        record.topic(), record.kafkaPartition(), record.kafkaOffset());
                }
                return;
            } catch (Throwable reportError) {
                if (reportError != cause) {
                    reportError.addSuppressed(cause);
                }
                if (!skipError) {
                    throw new RuntimeException(reportError);
                }
                cause = reportError;
            }
        }
        if (!skipError) {
            throw new RuntimeException(cause);
        }
        droppedRecords++;
        LOGGER.error("skip_error=true: dropping record {}-{}@{} without writing it to MaxCompute and without an "
                + "error reporter, so the committed offset will move past it with no destination for this record. "
                + "Configure both {} and {} to route such records to a dead-letter topic instead.",
            record.topic(), record.kafkaPartition(), record.kafkaOffset(),
            ConfigParameter.RUNTIME_ERROR_TOPIC_NAME.getName(),
            ConfigParameter.RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS.getName(),
            cause);
    }

    private static void clearRecord(Record record) {
        if (record == null) {
            return;
        }
        for (int i = 0; i < record.getColumnCount(); i++) {
            record.set(i, null);
        }
    }

    public synchronized Status flushAndReset() {
        long totalBytes = 0;
        // 所有计数都必须在 reset() 之前取快照：原来 return 读的是已被清零的 processedRecords，
        // 落盘条数恒为 0（SinkStatusContext 的累计与 Total write 日志跟着一起失真）。
        // 本项新增的 dropped/reported 与 preCommit 那句"写了多少条"都依赖这个顺序。
        // 注意：这与 PR #18 的同一处修复同源，先落的保留，另一个 rebase 后此 hunk 变为空。
        long flushedRecords = processedRecords;
        long flushedDropped = droppedRecords;
        long flushedReported = reportedRecords;
        droppedRecords = 0;
        reportedRecords = 0;
        if (streamSession != null && streamPack != null) {
            totalBytes = streamPack.getDataSize();
            try {
                streamPack.flush();
                LOGGER.info("Flush records from {} to {}.", startOffset, maxOffset);
            } catch (IOException e) {
                LOGGER.error("Failed to flush stream pack", e);
                throw new RuntimeException(e);
            }
            reset();
        }
        return new Status(maxOffset, flushedRecords, totalBytes, flushedDropped, flushedReported);
    }

    private void reset() {
        processedRecords = 0;
        batchInsertTime = -1;
        startOffset = -1;
        // droppedRecords / reportedRecords 不在此清零：它们随 Status 交给 flushAndReset 的调用方结转，
        // 在这里清零等于把计数丢掉，preCommit 就再也说不出"这次提交越过了几条没落盘的记录"。
    }

    private void initStreamUploadSession(long timestamp) throws OdpsException, IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Thread({}) Reset stream upload session, last timestamp: {}, current: {}",
                Thread.currentThread().getId(), partitionStartTime, timestamp);
        }
        setPartitionStartTime(timestamp);
        PartitionSpec partitionSpec = buildPartitionSpec(partitionStartTime);

        streamSession = tunnel.buildStreamUploadSession(project, table).setPartitionSpec(partitionSpec)
            .setCreatePartition(true).build();
        LOGGER.info("Thread({}) create streaming session {} successfully!", Thread.currentThread().getId(),
            streamSession.getId());
        streamPack = streamSession.newRecordPack(new CompressOption());
        reusedRecord = streamSession.newRecord();
    }

    private PartitionSpec buildPartitionSpec(long timestamp) {
        PartitionSpec partitionSpec = new PartitionSpec();
        ZonedDateTime dt = Instant.ofEpochSecond(timestamp).atZone(tz.toZoneId());

        if (useNewPartitionFormat) {
            switch (partitionWindowType) {
                case DAY:
                    partitionSpec.set(RecordConverter.PT, dt.format(DAY_FORMATTER));
                    break;
                case HOUR:
                    partitionSpec.set(RecordConverter.PT, dt.format(HOUR_FORMATTER));
                    break;
                case MINUTE:
                    partitionSpec.set(RecordConverter.PT, dt.format(MINUTE_FORMATTER));
                    break;
                default:
                    throw new RuntimeException("Unsupported partition window type");
            }
        } else {
            String datetimeString = dt.format(DATETIME_FORMATTER);
            switch (partitionWindowType) {
                case DAY:
                    partitionSpec.set(RecordConverter.PT, datetimeString.substring(0, 10));
                    break;
                case HOUR:
                    partitionSpec.set(RecordConverter.PT, datetimeString.substring(0, 13));
                    break;
                case MINUTE:
                    partitionSpec.set(RecordConverter.PT, datetimeString.substring(0, 16));
                    break;
                default:
                    throw new RuntimeException("Unsupported partition window type");
            }
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Generate partition spec: {}, timestamp {}", partitionSpec, timestamp);
        }

        return partitionSpec;
    }

    private long setPartitionStartTime(long timestamp) {
        ZonedDateTime dt = Instant.ofEpochSecond(timestamp).atZone(tz.toZoneId());
        int year = dt.getYear();
        int month = dt.getMonthValue();
        int day = dt.getDayOfMonth();
        int hour = dt.getHour();
        int minute = dt.getMinute();

        switch (partitionWindowType) {
            case DAY:
                hour = 0;
                minute = 0;
                break;
            case HOUR:
                minute = 0;
                break;
            case MINUTE:
                break;
            default:
                throw new RuntimeException("Unsupported partition window type");
        }
        ZonedDateTime dateTime = ZonedDateTime.of(year, month, day, hour, minute, 0, 0, tz.toZoneId());
        partitionStartTime = dateTime.toEpochSecond();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Thread({}) reset partition start time to {}", Thread.currentThread().getId(),
                partitionStartTime);
        }
        return partitionStartTime;
    }
}
