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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.kafka.KafkaWriter;
import com.aliyun.odps.kafka.connect.MaxComputeSinkConnectorConfig.BaseParameter;
import com.aliyun.odps.kafka.connect.converter.RecordConverter;
import com.aliyun.odps.kafka.connect.converter.RecordConverterBuilder;
import com.aliyun.odps.kafka.connect.utils.OdpsUtils;
import com.aliyun.odps.utils.StringUtils;

public class MaxComputeSinkTask extends SinkTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(MaxComputeSinkTask.class);
  private final ConcurrentHashMap<TopicPartition, SinkStatusContext>
    sinkStatus =
    new ConcurrentHashMap<>();
  private final Queue<Future<Boolean>> writerTasks = new LinkedList<>();

  private Odps odps;
  private String project;
  private String table;
  private RecordConverter recordConverter;

  private KafkaWriter runtimeErrorWriter = null;
  private ExecutorService executor; // used to execute multi-thread sinkWriter

  private boolean needSyncCommit = false;

  /*
    Performance metrics
   */
  private long totalBytesSentByClosedWriters = 0;
  private long startTimestamp;

  /**
   * For Account
   */
  private MaxComputeSinkConnectorConfig config;
  private long odpsCreateLastTime;
  private long timeout;
  private String accountType;
  private boolean useStreamTunnel;
  private int batchSize; // max_records in queue when sink writer can do run ;

  @Override
  public void start(Map<String, String> map) {
    LOGGER.info("Thread({}) Enter START", Thread.currentThread().getId());

    startTimestamp = System.currentTimeMillis();

    config = new MaxComputeSinkConnectorConfig(map);
    accountType = config.getString(BaseParameter.ACCOUNT_TYPE.getName());
    timeout = config.getLong(BaseParameter.CLIENT_TIMEOUT_MS.getName());

    String endpoint = config.getString(BaseParameter.MAXCOMPUTE_ENDPOINT.getName());
    project = config.getString(BaseParameter.MAXCOMPUTE_PROJECT.getName());
    table = config.getString(BaseParameter.MAXCOMPUTE_TABLE.getName());
    batchSize = config.getInt(BaseParameter.RECORD_BATCH_SIZE.getName());
    int poolSize = config.getInt(BaseParameter.POOL_SIZE.getName());
    executor = Executors.newFixedThreadPool(poolSize); // multi-thread to run record sink to MC
    // Init odps
    initOrRebuildOdps();
    // Init converter builder
    RecordConverterBuilder.Format format = RecordConverterBuilder.Format.valueOf(
      config.getString(BaseParameter.FORMAT.getName()));
    RecordConverterBuilder.Mode mode = RecordConverterBuilder.Mode.valueOf(
      config.getString(BaseParameter.MODE.getName()));

    RecordConverterBuilder converterBuilder = new RecordConverterBuilder();
    converterBuilder.format(format).mode(mode);
    converterBuilder.schema(odps.tables().get(table).getSchema());
    recordConverter = converterBuilder.build();

    useStreamTunnel = config.getBoolean(BaseParameter.USE_STREAM_TUNNEL.getName());

    if (useStreamTunnel) {
      LOGGER.info("MaxCompute Streaming Tunnel enable.");
    }

    if (
      !StringUtils.isNullOrEmpty(config.getString(BaseParameter.RUNTIME_ERROR_TOPIC_NAME.getName()))
      && !StringUtils.isNullOrEmpty(
        config.getString(BaseParameter.RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS.getName()))) {

      runtimeErrorWriter = new KafkaWriter(config);
      LOGGER.info("Thread({}) new runtime error kafka writer done", Thread.currentThread().getId());
    }

    LOGGER.info("Thread({}) Start MaxCompute sink task done", Thread.currentThread().getId());
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    LOGGER.info("Thread({}) Enter OPEN", Thread.currentThread().getId());
    for (TopicPartition partition : partitions) {
      LOGGER.info("OPEN (topic: {}, partition: {})", partition.topic(), partition.partition());
    }
    sinkStatus.clear();
    writerTasks.clear();
  }

  @Override
  public void put(Collection<SinkRecord> collection) {
    LOGGER.debug("Thread({}) Enter PUT", Thread.currentThread().getId());
    if (collection.isEmpty()) {
      LOGGER.info("Thread({}) Enter empty put records", Thread.currentThread().getId());
      return;
    } else {
      LOGGER.debug("Thread({}) putted records size {}", Thread.currentThread().getId(),
                   collection.size());
    }
    for (SinkRecord r : collection) {
      TopicPartition partition = new TopicPartition(r.topic(), r.kafkaPartition());
      SinkStatusContext sinkStatusContext = sinkStatus.get(partition);
      if (sinkStatusContext == null) {
        sinkStatusContext = new SinkStatusContext(batchSize);
        sinkStatus.put(partition, sinkStatusContext);
      }
      if (sinkStatusContext.containsOffset(r.kafkaOffset())) {
        continue;
      }
      List<SinkRecord> cur = sinkStatusContext.getRecordQueue();
      // 需要记录当前收到的分区 offset 最小值
      cur.add(r);
      if (cur.size() >= batchSize) {
        MaxComputeSinkWriter curWriter = createSinkWriter(cur, sinkStatusContext);
        writerTasks.add(executor.submit(curWriter));
        sinkStatusContext.resetRecordQueue();
      }
    }
    if (needSyncCommit) {
      LOGGER.info("Thread({}) Sync commit", Thread.currentThread().getId());
      super.context.requestCommit();
    }
  }

  @Override
  public final Map<TopicPartition, OffsetAndMetadata> preCommit(
    Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    LOGGER.info("Thread({}) PreCommit, currentOffsets {}", Thread.currentThread().getId(),
                currentOffsets);
    Map<TopicPartition, OffsetAndMetadata> toCommitOffsets = new HashMap<>();
    flush(currentOffsets);

    // 本地的offset 和 currentOffsets 进行比较; 告诉其哪些partition 哪些地方已经消费了可以提交

    boolean hasGap = false;
    for (Entry<TopicPartition, OffsetAndMetadata> entry : currentOffsets.entrySet()) {
      TopicPartition partition = entry.getKey();
      OffsetAndMetadata offsetAndMetadata = entry.getValue();
      SinkStatusContext curStatus = sinkStatus.get(partition);

      if (curStatus == null) {
        LOGGER.warn("no partition exist in innerOffsets");
        continue;
      }
      long consumedOffset = curStatus.mergeOffset();
      LOGGER.info(
        "PreCommit Partition {}, currentOffset {}, localOffsetRangeMap {}, localConsumedOffSet {}",
        partition, offsetAndMetadata.offset(), curStatus.getOffsetRangeMap(), consumedOffset + 1);

      // merge后还不为空，说明存在断档，需要在下次put操作后立刻提交
      hasGap |= !curStatus.isEmpty();
      if (consumedOffset != -1) {
        toCommitOffsets.put(partition,
                            new OffsetAndMetadata(consumedOffset + 1,
                                                  offsetAndMetadata.metadata()));
        LOGGER.info("partition:{} consumed offset: {}", partition, consumedOffset);
      }
    }

    needSyncCommit = hasGap;
    if (hasGap) {
      LOGGER.info("Has gap, need sync commit.");
    }
    return toCommitOffsets;
  }

  @Override
  public final void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    LOGGER.info("Thread({}) kafka flush, offsets {}", Thread.currentThread().getId(), offsets);
    if (LOGGER.isInfoEnabled()) {
      for (Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
        LOGGER.debug("Thread({}) FLUSH (topic: {}, partition: {})", Thread.currentThread().getId(),
                     entry.getKey().topic(), entry.getKey().partition());
      }
    }
    initOrRebuildOdps();
    // should be flush by offsets
    for (TopicPartition partition : offsets.keySet()) {
      //Entry<TopicPartition, SinkStatusContext> item : sinkStatus.entrySet()
      //TopicPartition partition = item.getKey();
      SinkStatusContext curContext = sinkStatus.get(partition);
      if (curContext == null) {
        continue;
      }
      List<SinkRecord> left = curContext.getRecordQueue();
      if (!left.isEmpty()) {
        try {
          MaxComputeSinkWriter curWriter = createSinkWriter(left, curContext);
          writerTasks.add(executor.submit(curWriter));
          curContext.resetRecordQueue();
        } catch (Throwable e) {
          LOGGER.error("flush buffer error,partition:{},error:{} ", partition, e.getMessage(), e);
        }
      }
    }

    if (writerTasks.isEmpty()) {
      LOGGER.info("no data written to tunnel!");
    }

    List<Future<Boolean>> errorFutures = new ArrayList<>();
    while (!writerTasks.isEmpty()) {
      Future<Boolean> f = null;
      boolean result = false;
      try {
        f = writerTasks.poll();
        result = f.get();
      } catch (Throwable e) {
        LOGGER.error("unrecoverable error happens: {} ,the task will exit! ", e.getMessage());
        errorFutures.add(f);
      }
      if (!result) {
        needSyncCommit = true;
      }
    }
    if (!errorFutures.isEmpty()) {
      LOGGER.error("There are {} batchs fail when flush", errorFutures.size());
      writerTasks.addAll(errorFutures);
    }

    if (LOGGER.isInfoEnabled()) {
      totalBytesSentByClosedWriters = 0;
      sinkStatus.forEach(
        (pt, cxt) -> totalBytesSentByClosedWriters += cxt.getTotalBytesSentByWriter());
      LOGGER.info("Total bytes written by multi-writer :{}", totalBytesSentByClosedWriters);
    }
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Enter CLOSE");
      for (TopicPartition partition : partitions) {
        LOGGER.info("Thread({}) CLOSE (topic: {}, partition: {})", Thread.currentThread().getId(),
                    partition.topic(), partition.partition());
      }
    }

    LOGGER.info("Close Thread({}) Total bytes sent: {}, elapsed time: {}",
                Thread.currentThread().getId(),
                totalBytesSentByClosedWriters, System.currentTimeMillis() - startTimestamp);
  }

  @Override
  public void stop() {
    LOGGER.info("Thread({}) Enter STOP", Thread.currentThread().getId());
    if (executor != null) {
      executor.shutdown();
    }
    // TODO
    // resetOffset
    LOGGER.info("Stop Thread({}) Total bytes sent: {}, elapsed time: {}",
                Thread.currentThread().getId(),
                totalBytesSentByClosedWriters, (System.currentTimeMillis()) - startTimestamp);
  }

  private void initOrRebuildOdps() {
    LOGGER.debug("Enter initOrRebuildOdps!");
    // Exit fast
    if (odps == null) {
      this.odps = OdpsUtils.getOdps(config);
      odpsCreateLastTime = System.currentTimeMillis();
    }
    if (!Account.AccountProvider.STS.name().equals(accountType)) {
      return;
    }
    long current = System.currentTimeMillis();
    if (current - odpsCreateLastTime > timeout) {
      LOGGER.info("STS AK timed out. Last: {}, current: {}", odpsCreateLastTime, current);
      this.odps = OdpsUtils.getOdps(config);
      odpsCreateLastTime = current;
      LOGGER.info("Account refreshed. Creation time: {}", current);
    }
  }

  /**
   * @return MaxComputeSinkWriter
   */
  private MaxComputeSinkWriter createSinkWriter(List<SinkRecord> records,
                                                SinkStatusContext sinkStatusContext) {
    initOrRebuildOdps();
    return new MaxComputeSinkWriter(this.odps, records, sinkStatusContext, config, project, table,
                                    recordConverter,
                                    useStreamTunnel, runtimeErrorWriter);
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }
}
