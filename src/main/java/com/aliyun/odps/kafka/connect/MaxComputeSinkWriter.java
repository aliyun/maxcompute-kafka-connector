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
import java.lang.reflect.Field;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.Callable;

import com.aliyun.odps.tunnel.io.StreamRecordPackImpl;
import org.apache.kafka.common.TopicPartition;
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
import com.aliyun.odps.kafka.connect.converter.RecordConverter;
import com.aliyun.odps.kafka.connect.utils.OdpsUtils;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;


public class MaxComputeSinkWriter implements Closeable, Callable<Boolean> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MaxComputeSinkWriter.class);

  private static final DateTimeFormatter DATETIME_FORMATTER =
      DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss");

  private static final int DEFAULT_RETRY_TIMES = 3;
  private static final int DEFAULT_RETRY_INTERVAL_SECONDS = 10;

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
  private List<SinkRecord> recordBuffer;
  private TopicPartition partition;
  private KafkaWriter errorReporter;
  private long processedRecordsEachEcho = 0;
  /*
    Configs of this sink writer, won't change
   */
  private Odps odps;
  private TableTunnel tunnel;
  private String project;
  private String tunnelEndpoint; // tunnel endpoint
  private String table;
  private int bufferSize;
  private RecordConverter converter;
  private PartitionWindowType partitionWindowType;
  private TimeZone tz;
  private int retryTimes;
  private boolean useStreamingTunnel = false;
  private SinkStatusContext sinkStatusContext;
  private boolean skipError = false;

  /*
    Performance metrics
   */
  private long totalBytesByClosedSessions = 0;

  private int getActualBufferBytes() {
    return bufferSize * 1024;
  }

  private TableTunnel.StreamRecordPack recreateRecordPack() throws IOException, TunnelException {
    return streamSession.newRecordPack(
        new CompressOption(CompressOption.CompressAlgorithm.ODPS_ZLIB, 1, 0));
  }

  public MaxComputeSinkWriter(
      MaxComputeSinkConnectorConfig config,
      String project,
      String table,
      RecordConverter converter,
      int bufferSize,
      PartitionWindowType partitionWindowType,
      TimeZone tz,
      boolean useStreamingTunnel,
      int retryTimes,
      String tunnelEndpoint) {
    this.odps = OdpsUtils.getOdps(config);
    this.odps.setUserAgent("aliyun-maxc-kafka-connector");
    this.tunnel = new TableTunnel(this.odps);
    this.project = Objects.requireNonNull(project);
    this.tunnelEndpoint = Objects.requireNonNull(tunnelEndpoint); // add tunnel endpoint config
    if (!Objects.equals(this.tunnelEndpoint, "")) {
      this.tunnel.setEndpoint(tunnelEndpoint);
    }
    this.table = Objects.requireNonNull(table);
    this.converter = Objects.requireNonNull(converter);
    this.bufferSize = bufferSize;
    this.partitionWindowType = partitionWindowType;
    this.tz = Objects.requireNonNull(tz);
    this.useStreamingTunnel = useStreamingTunnel;
    this.retryTimes = retryTimes;
    if (this.retryTimes < 0) {
      this.retryTimes = DEFAULT_RETRY_TIMES;
    }
  }

  public void write(SinkRecord sinkRecord, Long timestamp) throws IOException {
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

  private void writeToBatchWriter() throws IOException {
    writer.write(reusedRecord);
  }

  private void writeToStreamWriter() throws IOException {
    streamPack.append(reusedRecord);
    if (streamPack.getDataSize() >= getActualBufferBytes()) {
      flushStreamPackWithRetry(retryTimes);
    }
  }

  /**
   * Return the minimum uncommitted offset
   */
  public long getMinOffset() {
    return minOffset;
  }

  /**
   * Refresh the STS access key ID & secret.
   *
   * @param odps odps
   */
  public void refresh(Odps odps) {
    LOGGER.info("Enter refresh.");
    this.odps = odps;
    this.tunnel = new TableTunnel(odps);
    if (!Objects.equals(this.tunnelEndpoint, "")) {
      this.tunnel.setEndpoint(this.tunnelEndpoint);
    }

    // Upload session may not exist
    if (session != null) {
      String sessionId = session.getId();
      try {
        this.session = tunnel.getUploadSession(project, table, partitionSpec, sessionId);
      } catch (Exception e) {
        LOGGER.error("Set session failed!!!", e);
        throw new RuntimeException(e);
      }
    }
    if (streamSession != null) {
      try {
        // old version: streamSession = tunnel.createStreamUploadSession(project, table, partitionSpec, true);
        // new version: below
        streamSession =
            tunnel.buildStreamUploadSession(project, table)
                .setPartitionSpec(partitionSpec)
                .setCreatePartition(true)
                .build();
        if (streamPack != null) {
          try {
            StreamRecordPackImpl packImpl = (StreamRecordPackImpl) streamPack;
            Field sessionField = StreamRecordPackImpl.class.getDeclaredField("session");
            sessionField.setAccessible(true); // 破除 private 限制
            sessionField.set(packImpl, streamSession);
          } catch (Exception e) {
            LOGGER.error("Refresh sts token failed: cannot recreate stream pack", e);
            throw new RuntimeException(e);
          }
        }
        flushStreamPackWithRetry(retryTimes);
        streamPack = recreateRecordPack();
      } catch (TunnelException e) {
        LOGGER.error("Refresh sts token failed: cannot recreate stream session", e);
        throw new RuntimeException(e);
      } catch (IOException e) {
        LOGGER.error("Refresh sts token failed: cannot recreate stream pack", e);
        throw new RuntimeException(e);
      }
    }
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

  public void flush() {
    if (streamSession != null && streamPack != null) {
      try {
        flushStreamPackWithRetry(retryTimes);
      } catch (IOException e) {
        LOGGER.error("Failed to flush stream pack", e);
        throw new RuntimeException(e);
      }
    }
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
            "Failed to flush streaming pack, retrying after " + DEFAULT_RETRY_INTERVAL_SECONDS
            + "s", ex);
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

  private void closeCurrentNormalSessionWithRetry(int retryLimit) throws IOException {
    String threadName = String.valueOf(Thread.currentThread().getId());
    LOGGER.debug("Thread({}) Enter closeCurrentSessionWithRetry!", threadName);
    if (session == null) {
      return;
    }

    totalBytesByClosedSessions += ((TunnelBufferedWriter) writer).getTotalBytes();
    writer.close();
    LOGGER.debug("Thread({}) writer.close() successfully!", threadName);

    while (true) {
      try {
        session.commit();
        LOGGER.debug("Thread({}) session.commit() successfully!", threadName);
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
      LOGGER.info("Reset stream upload session, last timestamp: {}, current: {}",
                  partitionStartTimestamp, timestamp);
      // try flushing the pack
      flushStreamPackWithRetry(retryTimes);

      PartitionSpec partitionSpec = getPartitionSpec(timestamp);
      this.partitionSpec = partitionSpec;
      this.partitionStartTimestamp = null;
      resetPartitionStartTimestamp(timestamp);

      streamSession =
          tunnel.buildStreamUploadSession(project, table)
              .setPartitionSpec(partitionSpec)
              .setCreatePartition(true)
              .build();
      streamPack = recreateRecordPack();
      reusedRecord = streamSession.newRecord();
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
      writer = session.openBufferedWriter(true);
      reusedRecord = session.newRecord();
      ((TunnelBufferedWriter) writer).setBufferSize(getActualBufferBytes());
    }
  }

  private void resetUploadSessionIfNeeded(Long timestamp) throws OdpsException, IOException {
    if (useStreamingTunnel) {
      resetStreamUploadSessionIfNeeded(timestamp);
    } else {
      resetNormalUploadSessionIfNeeded(timestamp);
    }
  }

  private PartitionSpec getPartitionSpec(Long timestamp) {
    PartitionSpec partitionSpec = new PartitionSpec();
    ZonedDateTime dt = Instant.ofEpochSecond(timestamp).atZone(tz.toZoneId());
    String datetimeString = dt.format(DATETIME_FORMATTER);

    switch (partitionWindowType) {
      case DAY:
        partitionSpec.set("pt", datetimeString.substring(0, 10));
        break;
      case HOUR:
        partitionSpec.set("pt", datetimeString.substring(0, 13));
        break;
      case MINUTE:
        partitionSpec.set("pt", datetimeString.substring(0, 16));
        break;
      default:
        throw new RuntimeException("Unsupported partition window type");
    }

    return partitionSpec;
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

  private void resetPartitionStartTimestamp(Long timestamp) {
    if (partitionStartTimestamp == null) {
      ZonedDateTime dt = Instant.ofEpochSecond(timestamp).atZone(tz.toZoneId());
      ZonedDateTime partitionStartDatetime;
      switch (partitionWindowType) {
        case DAY:
          partitionStartDatetime =
              ZonedDateTime.of(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(),
                               0, 0, 0, 0, tz.toZoneId());
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
    }
  }

  private static synchronized void createPartition(
      Odps odps,
      String project,
      String table,
      PartitionSpec partitionSpec)
      throws OdpsException {
    Table t = odps.tables().get(project, table);
    // Check the existence of the partition before executing a DML. Could save a lot of time.
    if (!t.hasPartition(partitionSpec)) {
      // Add if not exists to avoid conflicts
      t.createPartition(partitionSpec, true);
    }
  }

  public void reset() {
    this.session = null;
    this.writer = null;
    this.partition = null;
    this.processedRecordsEachEcho = 0;
  }

  public void setSinkStatusContext(SinkStatusContext context) {
    sinkStatusContext = context;
  }

  public void setSkipError(boolean skip) {
    skipError = skip;
  }

  public void setRecordBuffer(List<SinkRecord> records) {
    this.recordBuffer = records;
  }


  public void setErrorReporter(KafkaWriter errorReporter) {
    this.errorReporter = errorReporter;
  }


  @Override
  public Boolean call() throws RuntimeException {
    long time = System.currentTimeMillis() / 1000;
    long start = -1;
    long end = -1;
    processedRecordsEachEcho = 0;
    boolean ok = true;
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
    } catch (RuntimeException e) {
      // 数据内部错误，且用户选择不跳过,直接抛给上层框架
      LOGGER.error("something error in MaxComputerSinkWriter : " + e.getMessage());
      throw new RuntimeException(e);
    }
    try {
      flush();
      close();
      if (start != -1) {
        sinkStatusContext.addOffsetInterval(start, end);
        sinkStatusContext.addTotalBytesSentByWriter(getTotalBytes());
      }
    } catch (IOException e) {
      LOGGER.warn("something error in tunnel close,Please check tunnel environment! {}",
                  e.getMessage());
      ok = false;
    }
    return ok;
  }
}
