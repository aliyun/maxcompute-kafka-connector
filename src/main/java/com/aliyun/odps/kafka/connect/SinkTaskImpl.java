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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.aliyun.odps.Odps;
import com.aliyun.odps.kafka.connect.converter.RecordConverter;
import com.aliyun.odps.kafka.connect.converter.RecordConverterBuilder;
import com.aliyun.odps.kafka.connect.sink.BufferedWriter;
import com.aliyun.odps.kafka.connect.utils.OdpsUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aliyun.odps.kafka.connect.ConfigParameter.CSV_DELIMITER;
import static com.aliyun.odps.kafka.connect.ConfigParameter.FORMAT;
import static com.aliyun.odps.kafka.connect.ConfigParameter.MAXCOMPUTE_PROJECT;
import static com.aliyun.odps.kafka.connect.ConfigParameter.MAXCOMPUTE_TABLE;
import static com.aliyun.odps.kafka.connect.ConfigParameter.MODE;
import static com.aliyun.odps.kafka.connect.ConfigParameter.RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS;
import static com.aliyun.odps.kafka.connect.ConfigParameter.RUNTIME_ERROR_TOPIC_NAME;

public class SinkTaskImpl extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkTaskImpl.class);
    private final ConcurrentHashMap<TopicPartition, SinkStatusContext> sinkStatus = new ConcurrentHashMap<>();
    private ConnectorConfig config;
    private Odps odps;
    private String project;
    private String table;
    private RecordConverter recordConverter;

    private long startTimestamp;

    /** 已随分区状态释放而结转的写入量，保证 printProcess 的口径不因重平衡归零。 */
    private final AtomicLong releasedProcessedBytes = new AtomicLong();
    private final AtomicLong releasedProcessedRecords = new AtomicLong();

    private ErrorReporter errorReporter = null;

    @Override
    public final void initialize(SinkTaskContext context) {
        super.initialize(context);
        LOGGER.info("Thread({}) Enter initialize", Thread.currentThread().getId());
    }

    @Override
    public void start(Map<String, String> map) {
        LOGGER.info("Thread({}) Enter START", Thread.currentThread().getId());

        startTimestamp = System.currentTimeMillis();

        config = new ConnectorConfig(map);
        project = MAXCOMPUTE_PROJECT.getString(config);
        table = MAXCOMPUTE_TABLE.getString(config);

        // Init odps
        this.odps = OdpsUtils.getOdps(config);
        // Init converter builder
        RecordConverterBuilder.Format format = RecordConverterBuilder.Format.valueOf(FORMAT.getString(config));
        RecordConverterBuilder.Mode mode = RecordConverterBuilder.Mode.valueOf(MODE.getString(config));

        RecordConverterBuilder converterBuilder = new RecordConverterBuilder();
        converterBuilder.format(format).mode(mode).csvDelimiter(CSV_DELIMITER.getString(config));
        converterBuilder.schema(odps.tables().get(table).getSchema());
        recordConverter = converterBuilder.build();

        if (StringUtils.isNotEmpty(RUNTIME_ERROR_TOPIC_NAME.getString(config)) && StringUtils.isNotEmpty(
            RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS.getString(config))) {
            errorReporter = new ErrorReporter(config);
            LOGGER.info("Thread({}) new runtime error Kafka writer done", Thread.currentThread().getId());
        }

        LOGGER.info("Thread({}) Start MaxCompute sink task done", Thread.currentThread().getId());
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Thread({}) Enter OPEN, newly assigned partitions: {}", Thread.currentThread().getId(),
                partitions);
        }

        // Kafka Connect 只把"本次新分配给本 task"的分区交给 open()，两类分区的旧状态含义完全不同：
        //   1) 出现在参数里的分区：worker 已把它的消费位点重置回已提交位置（重新分配必然重放），
        //      缓冲区里的记录稍后会再投递一次，此处落盘只会造成重复 —— 直接释放；
        //   2) 不在参数里但仍归本 task 的分区（cooperative 重平衡下保留的分区）：位点不会回退，也不会被
        //      重放。原实现无条件 sinkStatus.clear()，这批"已消费、未落盘"的记录就永久丢了，而后续提交
        //      水位还会继续越过它们 —— 必须先落盘，且状态保留给下一次 preCommit 继续推进水位。
        Set<TopicPartition> reassigned = new HashSet<>(partitions);
        Set<TopicPartition> assigned = assignedPartitions();
        Iterator<Entry<TopicPartition, SinkStatusContext>> entries = sinkStatus.entrySet().iterator();
        while (entries.hasNext()) {
            Entry<TopicPartition, SinkStatusContext> entry = entries.next();
            TopicPartition partition = entry.getKey();
            SinkStatusContext status = entry.getValue();

            if (reassigned.contains(partition)) {
                LOGGER.info("Thread({}) open(): release {} without flushing, its records will be re-delivered",
                    Thread.currentThread().getId(), partition);
                release(status);
                entries.remove();
                continue;
            }

            boolean flushed = status.tryFlushForRelease();
            if (assigned == null || assigned.contains(partition)) {
                // 仍归本 task：位点不会回退，状态必须保留。落盘成功后由下一次 preCommit 正常推进水位；
                // 落盘失败则把缓冲区留给下一次 preCommit 重试，绝不丢弃已消费的记录。
                if (!flushed) {
                    LOGGER.error("Thread({}) open(): keep {} because buffered records are not persisted yet,"
                        + " will retry at the next preCommit", Thread.currentThread().getId(), partition);
                }
                continue;
            }

            if (!flushed) {
                LOGGER.warn("Thread({}) open(): dropping unpersisted records of {}; this partition is no longer"
                    + " assigned to this task, so its new owner re-reads it from the last committed offset",
                    Thread.currentThread().getId(), partition);
            }
            release(status);
            entries.remove();
        }
        printProcess();
    }

    /** 本 task 当前持有的分区集合；读不到时返回 null，调用方按保守分支处理。 */
    private Set<TopicPartition> assignedPartitions() {
        if (context == null) {
            return null;
        }
        try {
            return context.assignment();
        } catch (RuntimeException e) {
            LOGGER.warn("Thread({}) cannot read task assignment at open(): {}", Thread.currentThread().getId(),
                e.toString());
            return null;
        }
    }

    private void release(SinkStatusContext status) {
        releasedProcessedBytes.addAndGet(status.getProcessedBytes());
        releasedProcessedRecords.addAndGet(status.getProcessedRecords());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Thread({}) Enter PUT", Thread.currentThread().getId());
        }

        if (records.isEmpty()) {
            LOGGER.info("Thread({}) Enter empty put records", Thread.currentThread().getId());
            return;
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Thread({}) putted records size {}", Thread.currentThread().getId(), records.size());
            }
        }
        boolean reachBufferLimit = false;
        for (SinkRecord r : records) {
            TopicPartition partition = new TopicPartition(r.topic(), r.kafkaPartition());
            SinkStatusContext sinkStatusContext = sinkStatus.get(partition);
            if (sinkStatusContext == null) {
                sinkStatusContext = new SinkStatusContext(newBufferedWriter());
                sinkStatus.put(partition, sinkStatusContext);
            }
            reachBufferLimit |= sinkStatusContext.putRecord(r);
        }
        if (reachBufferLimit) {
            super.context.requestCommit();
        }
    }

    @Override
    public final Map<TopicPartition, OffsetAndMetadata> preCommit(
        Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Thread({}) PreCommit, currentOffsets {}", Thread.currentThread().getId(), currentOffsets);
        }

        Map<TopicPartition, OffsetAndMetadata> toCommitOffsets = new HashMap<>();

        for (Entry<TopicPartition, OffsetAndMetadata> entry : currentOffsets.entrySet()) {
            TopicPartition partition = entry.getKey();
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            SinkStatusContext curStatus = sinkStatus.get(partition);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Thread({}) preCommit (topic: {}, partition: {})", Thread.currentThread().getId(),
                    entry.getKey().topic(), entry.getKey().partition());
            }

            if (curStatus == null) {
                LOGGER.warn("no partition exist in innerOffsets");
                continue;
            }
            long consumedOffset = curStatus.flush();
            LOGGER.info("PreCommit Partition {}, currentOffset {}, localConsumedOffSet {}", partition,
                offsetAndMetadata.offset(), consumedOffset + 1);

            if (consumedOffset != -1) {
                toCommitOffsets.put(partition, new OffsetAndMetadata(consumedOffset + 1, offsetAndMetadata.metadata()));
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("partition:{} consumed offset: {}", partition, consumedOffset);
                }
            }
        }
        // 这里故意使用isDebugEnable来判断，减少process log规模
        if (LOGGER.isDebugEnabled()) {
            printProcess();
        }

        return toCommitOffsets;
    }

    /**
     * 为一个 Kafka 分区创建写入器。默认实现与原先内联 new 完全一致；独立出来只为让用例能在
     * 不连接 MaxCompute 的情况下驱动 put()/preCommit() 的故障路径（见 SinkTaskOffsetCommitRecoveryTest）。
     */
    protected BufferedWriter newBufferedWriter() {
        return new BufferedWriter(this.odps, config, project, table, recordConverter, errorReporter);
    }

    @Override
    public final void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        LOGGER.info("Thread({}) Kafka flush, offsets {}", Thread.currentThread().getId(), offsets);
    }

    @Override
    public void stop() {
        LOGGER.info("Thread({}) Enter stop,elapsed time: {}", Thread.currentThread().getId(),
            System.currentTimeMillis() - startTimestamp);

        printProcess();
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Enter close, partitions: {}, elapsed time: {}", partitions,
                System.currentTimeMillis() - startTimestamp);
        }

        // worker 在两种情况下回调 close()：正常 revoke（closing commit 已经落盘并提交过）和分区 lost
        // （无法提交）。两者之后都会把这些分区的位点重置回已提交位置，记录一定会重放，所以这里只释放
        // 状态、不落盘。原实现什么都不清理，writer 会一直留到下一次 open() 被整表清空。
        for (TopicPartition partition : partitions) {
            SinkStatusContext status = sinkStatus.remove(partition);
            if (status != null) {
                release(status);
            }
        }

        printProcess();
    }

    private void printProcess() {
        AtomicLong totalProcessedBytes = new AtomicLong(releasedProcessedBytes.get());
        AtomicLong totalProcessedRecords = new AtomicLong(releasedProcessedRecords.get());
        sinkStatus.forEach((pt, cxt) -> {
            totalProcessedBytes.addAndGet(cxt.getProcessedBytes());
            totalProcessedRecords.addAndGet(cxt.getProcessedRecords());
        });
        LOGGER.info("Total write {} bytes,{} records", totalProcessedBytes, totalProcessedRecords);
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }
}
