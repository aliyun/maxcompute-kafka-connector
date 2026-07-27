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
import java.util.Map;
import java.util.Map.Entry;
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
        LOGGER.info("Thread({}) Enter OPEN", Thread.currentThread().getId());
        for (TopicPartition partition : partitions) {
            LOGGER.info("OPEN (topic: {}, partition: {})", partition.topic(), partition.partition());
        }

        sinkStatus.clear();
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
                BufferedWriter writer = new BufferedWriter(this.odps, config, project, table, recordConverter,
                    errorReporter);
                sinkStatusContext = new SinkStatusContext(writer);
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
            LOGGER.info("Enter close,elapsed time: {}", System.currentTimeMillis() - startTimestamp);
        }

        printProcess();
    }

    private void printProcess() {
        AtomicLong totalProcessedBytes = new AtomicLong();
        AtomicLong totalProcessedRecords = new AtomicLong();
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
