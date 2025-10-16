/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package com.aliyun.odps.kafka.connect;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.Callable;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.kafka.KafkaWriter;
import com.aliyun.odps.kafka.connect.MaxComputeSinkConnectorConfig.BaseParameter;
import com.aliyun.odps.kafka.connect.converter.RecordConverter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;

public class MaxComputeSinkWriter implements Closeable, Callable<Boolean> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MaxComputeSinkWriter.class);
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss");
    private static final DateTimeFormatter DAY_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH");
    private static final DateTimeFormatter MINUTE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

  private static final int DEFAULT_RETRY_TIMES = 3;
  private static final int DEFAULT_RETRY_INTERVAL_SECONDS = 10;
  private final List<SinkRecord> recordBuffer;

  /*
    Internal states of this sink writer, could change
   */
  private boolean needResetPartition = false;
  private Long minOffset = null;
  private UploadSession session;
  private TableTunnel.StreamUploadSession streamSession;
  private TableTunnel.StreamRecordPack streamPack;
  private PartitionSpec partitionSpec;
  private RecordWriter writer;
  private Record reusedRecord;
  private Long partitionStartTimestamp;
  private final KafkaWriter errorReporter;
  private final PartitionWindowType partitionWindowType;
  private final TimeZone tz;
  /*
    Configs of this sink writer, won't change
   */
  private Odps odps;
  private TableTunnel tunnel;
  private String project;
  private String tunnelEndpoint; // tunnel endpoint
  private String table;
  private final boolean useStreamingTunnel;
  private RecordConverter converter;
  private final SinkStatusContext sinkStatusContext;
  private final boolean useNewPartitionFormat;
  private final boolean skipError;
  private int retryTimes;
  private final long recordSize;
  private int processedRecordsEachEcho = 0;
  private final int bufferSizeKB;

  /*
    Performance metrics
   */
  private long totalBytesByClosedSessions = 0;

  public MaxComputeSinkWriter(Odps odps, List<SinkRecord> records,
                              SinkStatusContext sinkStatusContext,
                              MaxComputeSinkConnectorConfig config,
                              String project, String table, RecordConverter converter,
                              boolean useStreamingTunnel,
                              KafkaWriter errorReporter) {
    this.recordSize = records.size();
    this.recordBuffer = records;
    this.sinkStatusContext = sinkStatusContext;
    this.odps = odps;
    this.tunnel = new TableTunnel(this.odps);
    this.project = Objects.requireNonNull(project);
    this.tunnelEndpoint = Objects.requireNonNull(
      config.getString(BaseParameter.TUNNEL_ENDPOINT.getName())); // add tunnel endpoint config
    if (!Objects.equals(this.tunnelEndpoint, "")) {
      this.tunnel.setEndpoint(tunnelEndpoint);
    }
    this.table = Objects.requireNonNull(table);
    this.converter = Objects.requireNonNull(converter);
    this.bufferSizeKB = config.getInt(BaseParameter.BUFFER_SIZE_KB.getName());
    this.partitionWindowType = PartitionWindowType.valueOf(
      config.getString(BaseParameter.PARTITION_WINDOW_TYPE.getName()));

    this.useNewPartitionFormat =
      config.getBoolean(BaseParameter.USE_NEW_PARTITION_FORMAT.getName());
    this.tz =
      Objects.requireNonNull(
        TimeZone.getTimeZone(config.getString(BaseParameter.TIME_ZONE.getName())));
    this.useStreamingTunnel = useStreamingTunnel;
    this.retryTimes = config.getInt(BaseParameter.FAIL_RETRY_TIMES.getName());

    if (this.retryTimes < 0) {
      this.retryTimes = DEFAULT_RETRY_TIMES;
    }

    this.skipError = config.getBoolean(BaseParameter.SKIP_ERROR.getName());
    this.errorReporter = errorReporter;
  }

  private static synchronized void createPartition(Odps odps, String project, String table,
                                                   PartitionSpec partitionSpec)
    throws OdpsException {
    Table t = odps.tables().get(project, table);
    // Check the existence of the partition before executing a DML. Could save a lot of time.
    if (!t.hasPartition(partitionSpec)) {
      // Add if not exists to avoid conflicts
      t.createPartition(partitionSpec, true);
    }
  }

  @Override
  public Boolean call() throws RuntimeException {
    long time = System.currentTimeMillis() / 1000;
    long start = -1;
    long end = -1;
    processedRecordsEachEcho = 0;
    boolean ok = true;
    // TODO split batch tunnel and streaming tunnel
    try {
      for (SinkRecord record : recordBuffer) {

        write(record, time);
        if (start == -1) {
          start = record.kafkaOffset();
        }
        end = Math.max(end, record.kafkaOffset());
        processedRecordsEachEcho++;
      }
    } catch (IOException e) {
      // tunnel 的波动引起 , 会不断重试
      LOGGER.warn("something error in tunnel write,Please check tunnel environment! {}",
                  e.getMessage());
      ok = false;
    } catch (Throwable e) {
      // 数据内部错误，且用户选择不跳过,直接抛给上层框架
      LOGGER.error("something error in MaxComputerSinkWriter ", e);
      throw new RuntimeException(e);
    }
    try {
      flush();
      close();
      LOGGER.info("Flush {} records, from {} to {}", recordSize, start, end);
      if (start != -1) {
        sinkStatusContext.addOffsetRange(start, end);
        sinkStatusContext.addTotalBytesSentByWriter(getTotalBytes());
        sinkStatusContext.addProcessedRecords(processedRecordsEachEcho);
      }
    } catch (IOException e) {
      LOGGER.warn("something error in tunnel close,Please check tunnel environment! {}",
                  e.getMessage());
      ok = false;
    }
    return ok;
  }

  private void writeToBatchWriter() throws IOException {
    writer.write(reusedRecord);
  }

  private void writeToStreamWriter() throws IOException {
    streamPack.append(reusedRecord);
    if (streamPack.getDataSize() >= getActualBufferBytes()) {
      flushStreamPackWithRetry(retryTimes);
    }
  }

  private void write(SinkRecord sinkRecord, Long timestamp) throws IOException {
    if (minOffset == null) {
      minOffset = sinkRecord.kafkaOffset();
    }

    try {
      resetUploadSessionIfNeeded(timestamp);
    } catch (OdpsException e) {
      throw new IOException(e);
    }

    try {
      converter.convert(sinkRecord, reusedRecord);
    } catch (Exception e) {
      if (errorReporter != null) {
        errorReporter.write(sinkRecord);
        return;
      } else {
        if (skipError) {
          return;
        }
        throw new RuntimeException(e);
      }
    }
    if (useStreamingTunnel) {
      writeToStreamWriter();
    } else {
      writeToBatchWriter();
    }
  }

  private int getActualBufferBytes() {
    return bufferSizeKB * 1024;
  }

  /**
   * Return the minimum uncommitted offset
   */
  public Long getMinOffset() {
    return minOffset;
  }

  /**
   * Close the writer and commit data to MaxCompute
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Enter Writer.close()!");
    }
    closeCurrentSessionWithRetry(retryTimes);
  }

  private TableTunnel.StreamRecordPack recreateRecordPack() throws IOException, TunnelException {
    return streamSession.newRecordPack(
      new CompressOption(CompressOption.CompressAlgorithm.ODPS_ZLIB, 1, 0));
  }

  public long getTotalBytes() {
    if (writer != null) {
      try {
        return totalBytesByClosedSessions + ((TunnelBufferedWriter) writer).getTotalBytes();
      } catch (IOException e) {
        // Writer has been closed, ignore
      }
    }

    return totalBytesByClosedSessions;
  }

  private void flush() {
    if (streamSession != null && streamPack != null) {
      try {
        flushStreamPackWithRetry(retryTimes);
      } catch (IOException e) {
        LOGGER.error("Failed to flush stream pack", e);
        throw new RuntimeException(e);
      }
    }
  }

  private void closeCurrentStreamSessionWithRetry(int retryLimit) throws IOException {
    // stream session does not require closing, but we should check for remaining records.
    flushStreamPackWithRetry(retryLimit);
  }

  private void closeCurrentSessionWithRetry(int retryLimit) throws IOException {
    if (useStreamingTunnel) {
      closeCurrentStreamSessionWithRetry(retryLimit);
    } else {
      closeCurrentNormalSessionWithRetry(retryLimit);
    }
  }

  private void flushStreamPackWithRetry(int retryLimit) throws IOException {
    if (streamPack == null) {
      // init condition
      return;
    }
    int retried = 0;
    while (true) {
      try {
        streamPack.flush();
        break;
      } catch (IOException ex) {
        LOGGER.warn(
          "Failed to flush streaming pack, retrying after " + DEFAULT_RETRY_INTERVAL_SECONDS + "s",
          ex);
        try {
          Thread.sleep(DEFAULT_RETRY_INTERVAL_SECONDS * 1000);
        } catch (InterruptedException e) {
          LOGGER.warn("Retry sleep is interrupted, retry immediately", e);
        }
        retried++;
        if (retried >= retryLimit) {
          try {
            streamPack = recreateRecordPack();
          } catch (TunnelException e) {
            LOGGER.error("Failed to flush streaming pack after specified retries.", ex);
            throw new IOException("Failed to recreate stream pack on failed flushes.", e);
          }
          throw ex;
        }
      }
    }
    minOffset = null; // flush good
  }

  private void closeCurrentNormalSessionWithRetry(int retryLimit) throws IOException {
    String threadId = String.valueOf(Thread.currentThread().getId());
    LOGGER.debug("Thread({}) Enter closeCurrentSessionWithRetry!", threadId);
    if (session == null) {
      return;
    }

    totalBytesByClosedSessions += ((TunnelBufferedWriter) writer).getTotalBytes();
    writer.close();
    LOGGER.debug("Thread({}) writer.close() successfully!", threadId);

    while (true) {
      try {
        session.commit();
        LOGGER.info("Thread({}) session {} commit successfully!", threadId, session.getId());
        minOffset = null; // flush good
        break;
      } catch (TunnelException e) {
        // TODO: random backoff
        retryLimit -= 1;
        LOGGER.debug(String.format("retryLimit: %d", retryLimit));
        if (retryLimit >= 0) {
          try {
            Thread.sleep(DEFAULT_RETRY_INTERVAL_SECONDS * 1000);
          } catch (InterruptedException ex) {
            LOGGER.warn("Retry sleep is interrupted, retry immediately", ex);
          }
          LOGGER.warn("Failed to commit upload session, retrying", e);
        } else {
          throw new IOException(e);
        }
      }
    }

  }

  private void resetStreamUploadSessionIfNeeded(Long timestamp) throws OdpsException, IOException {
    if (needToResetUploadSession(timestamp)) {
      LOGGER.info("Thread({}) Reset stream upload session, last timestamp: {}, current: {}",
                  Thread.currentThread().getId(),
                  partitionStartTimestamp,
                  timestamp);
      // try flushing the pack
      flushStreamPackWithRetry(retryTimes);

      PartitionSpec partitionSpec = getPartitionSpec(timestamp);
      this.partitionSpec = partitionSpec;
      this.partitionStartTimestamp = null;
      resetPartitionStartTimestamp(timestamp);

      streamSession =
        tunnel.buildStreamUploadSession(project, table).setPartitionSpec(partitionSpec)
          .setCreatePartition(true).build();
      LOGGER.info("Thread({}) create streaming session {} successfully!",
                  Thread.currentThread().getId(), streamSession.getId());
      streamPack = recreateRecordPack();
      reusedRecord = streamSession.newRecord();
    }
  }

  private void resetUploadSessionIfNeeded(Long timestamp) throws OdpsException, IOException {
    if (useStreamingTunnel) {
      resetStreamUploadSessionIfNeeded(timestamp);
    } else {
      resetNormalUploadSessionIfNeeded(timestamp);
    }
  }

  private void resetNormalUploadSessionIfNeeded(Long timestamp) throws OdpsException, IOException {
    if (needToResetUploadSession(timestamp)) {
      closeCurrentSessionWithRetry(retryTimes);

      if (needResetPartition) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Reset upload session and odps partition, last timestamp: {}, current: {}",
                       partitionStartTimestamp, timestamp);
        }
        PartitionSpec partitionSpec = getPartitionSpec(timestamp);
        createPartition(odps, project, table, partitionSpec);
        this.partitionSpec = partitionSpec;
        this.partitionStartTimestamp = null;
        resetPartitionStartTimestamp(timestamp);
      }

      session = tunnel.createUploadSession(project, table, partitionSpec);
      LOGGER.info("Thread({}) create batch session {} successfully!",
                  Thread.currentThread().getId(), session.getId());
      writer = session.openBufferedWriter(true);
      reusedRecord = session.newRecord();
      ((TunnelBufferedWriter) writer).setBufferSize(getActualBufferBytes());
    }
  }

  private boolean needToResetUploadSession(Long timestamp) {
    if (partitionStartTimestamp != null) {
      switch (partitionWindowType) {
        case DAY:
          needResetPartition = timestamp >= partitionStartTimestamp + 24 * 60 * 60;
          break;
        case HOUR:
          needResetPartition = timestamp >= partitionStartTimestamp + 60 * 60;
          break;
        case MINUTE:
          needResetPartition = timestamp >= partitionStartTimestamp + 60;
          break;
        default:
          throw new RuntimeException("Unsupported partition window type");
      }
    } else {
      needResetPartition = true;
    }

    if (session == null && !useStreamingTunnel) {
      return true;
    }
    if (streamSession == null && useStreamingTunnel) {
      return true;
    }

    return needResetPartition;
  }

  private PartitionSpec getPartitionSpec(Long timestamp) {
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

    LOGGER.info("Generate partition spec: {}, timestamp {}", partitionSpec, timestamp);

    return partitionSpec;
  }

  private void resetPartitionStartTimestamp(Long timestamp) {
    if (partitionStartTimestamp == null) {
      ZonedDateTime dt = Instant.ofEpochSecond(timestamp).atZone(tz.toZoneId());
      ZonedDateTime partitionStartDatetime;
      switch (partitionWindowType) {
        case DAY:
          partitionStartDatetime =
            ZonedDateTime.of(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), 0,
                             0, 0, 0, tz.toZoneId());
          break;
        case HOUR:
          partitionStartDatetime =
            ZonedDateTime.of(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(),
                             dt.getHour(), 0, 0, 0, tz.toZoneId());
          break;
        case MINUTE:
          partitionStartDatetime =
            ZonedDateTime.of(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(),
                             dt.getHour(), dt.getMinute(), 0, 0, tz.toZoneId());
          break;
        default:
          throw new RuntimeException("Unsupported partition window type");
      }

      partitionStartTimestamp = partitionStartDatetime.toEpochSecond();

      LOGGER.info("Thread({}) reset partition start timestamp to {}",
                  Thread.currentThread().getId(), partitionStartTimestamp);
    }
  }
}
