package com.aliyun.odps.kafka.connect.sink;

import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    /** 累计已确认投递到错误 topic 的条数（Future 正常完成），用于把"排队"与"送达"分开说明。 */
    private long confirmedReportedRecords = 0;
    /**
     * 已交给上报通道、但还没有拿到投递凭据的记录：offset → 待确认的投递。
     *
     * <p>{@code ErrantRecordReporter#report} 是异步的：返回的 Future 可能稍后才异常完成。修复前
     * {@code handleFailedRecord()} 在入队那一刻就把记录记成"有去处"，水位照样推进；broker 随后
     * 拒收时只在回调里打了一行日志，于是这条记录既不在 MaxCompute、也不在错误 topic，而已提交的
     * offset 让它无法重放。现在未确认的记录留在这里，并且挡在提交水位前面。
     */
    private final SortedMap<Long, PendingReport> pendingReports = new TreeMap<Long, PendingReport>();

    /**
     * 一次 flush 里等在途投递收尾的最长时间。
     *
     * <p>刻意取短：{@code flushAndReset()} 发生在 {@code preCommit()} 的调用链上，等太久就是拖住 worker
     * 的提交循环。超时不是失败 —— 没等到收尾的记录继续挡在水位前面，下一次提交再等一轮；等待只让
     * "本次能确认的尽量确认"，不等待也不会丢数据。
     */
    static final long REPORT_SETTLE_TIMEOUT_MS = 5_000L;
    /** 可缩短的实例副本：回归用例要模拟"还在途"，不能真的等 30 秒。 */
    private long reportSettleTimeoutMs = REPORT_SETTLE_TIMEOUT_MS;

    /** 一条待确认记录：需要保留原始记录与错误，投递失败后还能重投。 */
    private static final class PendingReport {
        private final SinkRecord record;
        private final Throwable cause;
        private Future<Void> delivery;
        private boolean retried;

        private PendingReport(SinkRecord record, Throwable cause, Future<Void> delivery) {
            this.record = record;
            this.cause = cause;
            this.delivery = delivery;
        }
    }

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
     * 自上次成功落盘以来是否还有未持久化的记录。调用方据此判断丢弃该 writer 是否等于丢数据。
     */
    public synchronized boolean hasPendingData() {
        return processedRecords > 0;
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
                Future<Void> delivery = errorReporter.report(record, cause);
                pendingReports.put(record.kafkaOffset(), new PendingReport(record, cause, delivery));
                reportedRecords++;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Reported record {}-{}@{} to the error topic instead of writing it; the "
                            + "committed offset will not move past it until the send is confirmed",
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
        // 先收尾在途的错误上报，再决定这次能把水位推到哪：顺序反了就是把"未确认"当成"已投递"。
        settlePendingReports();
        // 所有计数都必须在 reset() 之前取快照：原来 return 读的是已被清零的 processedRecords，
        // 落盘条数恒为 0（SinkStatusContext 的累计与 Total write 日志跟着一起失真）。
        // dropped/reported 的结转与 preCommit 那句"写了多少条"都依赖这个顺序。
        // PR #18 与本项在 flushAndReset() 上是同一处修复：先落的保留，rebase 后合成这一段。
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
        // 水位只能停在第一条未确认记录之前。它下面的记录会被重新消费：已落盘的那些因此可能重复写一次
        // （本连接器本来就是 at-least-once），而未确认的那条要么之后送达、要么一直挡着 —— 两个结果都比
        // "记录哪儿也没去、位点却过去了"好。
        long heldAtOffset = pendingReports.isEmpty() ? -1L : pendingReports.firstKey();
        long commitableOffset = heldAtOffset < 0 ? maxOffset : Math.min(maxOffset, heldAtOffset - 1);
        if (heldAtOffset >= 0) {
            LOGGER.warn("Commit offset held at {} (highest record seen was {}): {} record(s) routed to the "
                    + "runtime error topic have no confirmed delivery yet. They are re-attempted at the next "
                    + "flush; until then the watermark cannot move past them, so records already written above "
                    + "them may be written again on replay.",
                heldAtOffset, maxOffset, pendingReports.size());
        }
        return new Status(commitableOffset, flushedRecords, totalBytes, flushedDropped, flushedReported,
            pendingReports.size(), heldAtOffset);
    }

    /**
     * 收一轮投递凭据：正常完成的移出等待队列；异常完成的立刻重投一次（并继续挡在水位前）；
     * 还在途的用剩余预算等一次，超时则原样留着。没有 Future 可依据的上报（返回 null）按未确认处理 ——
     * 拿不到凭据就不能算送达。
     */
    private void settlePendingReports() {
        if (pendingReports.isEmpty()) {
            return;
        }
        long deadline = System.currentTimeMillis() + reportSettleTimeoutMs;
        Iterator<Map.Entry<Long, PendingReport>> iterator = pendingReports.entrySet().iterator();
        int delivered = 0;
        int requeued = 0;
        while (iterator.hasNext()) {
            Map.Entry<Long, PendingReport> entry = iterator.next();
            PendingReport pending = entry.getValue();
            SinkRecord failed = pending.record;
            if (pending.delivery == null) {
                LOGGER.error("Record {}-{}@{} was routed to the error topic but the reporter returned no "
                        + "delivery handle, so its delivery cannot be confirmed; the committed offset stays "
                        + "before it.", failed.topic(), failed.kafkaPartition(), failed.kafkaOffset());
                continue;
            }
            long remainingMillis = deadline - System.currentTimeMillis();
            if (remainingMillis <= 0 && !pending.delivery.isDone()) {
                continue;
            }
            try {
                if (remainingMillis > 0) {
                    pending.delivery.get(remainingMillis, TimeUnit.MILLISECONDS);
                } else {
                    pending.delivery.get();
                }
            } catch (TimeoutException stillInFlight) {
                LOGGER.info("Record {}-{}@{} is still in flight to the error topic after {}ms; the committed "
                        + "offset stays before it.", failed.topic(), failed.kafkaPartition(),
                    failed.kafkaOffset(), reportSettleTimeoutMs);
                continue;
            } catch (ExecutionException | InterruptedException | CancellationException deliveryFailure) {
                if (deliveryFailure instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    continue;
                }
                if (pending.retried) {
                    LOGGER.error("Re-attempted report of record {}-{}@{} to the error topic failed as well; "
                            + "the committed offset stays before it so the record can be replayed.",
                        failed.topic(), failed.kafkaPartition(), failed.kafkaOffset(), deliveryFailure);
                    continue;
                }
                pending.retried = true;
                try {
                    pending.delivery = errorReporter.report(failed, pending.cause);
                    requeued++;
                    LOGGER.warn("Report of record {}-{}@{} to the error topic was rejected ({}); re-attempted "
                            + "once, and the committed offset stays before it until a send is confirmed.",
                        failed.topic(), failed.kafkaPartition(), failed.kafkaOffset(),
                        String.valueOf(deliveryFailure.getMessage()));
                } catch (Throwable retryError) {
                    LOGGER.error("Cannot even re-attempt the report of record {}-{}@{}: {}; the committed "
                            + "offset stays before it.", failed.topic(), failed.kafkaPartition(),
                        failed.kafkaOffset(), retryError.toString());
                }
                continue;
            }
            iterator.remove();
            delivered++;
            confirmedReportedRecords++;
        }
        if (delivered > 0 || requeued > 0) {
            LOGGER.info("Error-topic deliveries settled this flush: confirmed {}, re-attempted {}, still "
                    + "unconfirmed {}", delivered, requeued, pendingReports.size());
        }
    }

    /** 只在同包的回归用例里使用：把等待在途投递的预算改成毫秒级。 */
    void setReportSettleTimeoutMs(long timeoutMs) {
        this.reportSettleTimeoutMs = timeoutMs;
    }

    /** 尚未确认投递的记录条数；调用方用它判断"这次提交为什么没把水位推满"。 */
    public synchronized long getUnconfirmedReportedRecords() {
        return pendingReports.size();
    }

    private void reset() {
        processedRecords = 0;
        batchInsertTime = -1;
        startOffset = -1;
        // pendingReports 不在此清空：它们是跨窗口的投递凭据，清空等于把未确认的记录当作已送达。
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
