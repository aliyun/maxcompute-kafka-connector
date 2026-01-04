package com.aliyun.odps.kafka.connect.sink;

import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.TimeZone;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
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

    public BufferedWriter(Odps odps, ConnectorConfig config, String project, String table, RecordConverter converter,
        ErrantRecordReporter errorReporter) {

        this.tunnel = OdpsUtils.getTableTunnel(odps, config);
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
            converter.convert(record, reusedRecord);
            streamPack.append(reusedRecord);
            processedRecords++;
        } catch (Throwable e) {
            if (errorReporter != null) {
                errorReporter.report(record, e);
            } else if (!skipError) {
                throw new RuntimeException(e);
            }
        }
        maxOffset = Math.max(maxOffset, record.kafkaOffset());
        return streamPack.getDataSize() >= bufferLimitBytes;
    }

    public synchronized Status flushAndReset() {
        long totalBytes = 0;
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
        return new Status(maxOffset, processedRecords, totalBytes);
    }

    private void reset() {
        processedRecords = 0;
        batchInsertTime = -1;
        startOffset = -1;
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
