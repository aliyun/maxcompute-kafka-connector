package com.aliyun.odps.kafka.connect.SinkConnectorTest;

import java.util.Collection;
import java.util.Map;

import com.aliyun.odps.kafka.connect.SinkTaskImpl;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.integration.RuntimeHandles;
import org.apache.kafka.connect.integration.TaskHandle;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMCSinkTaskImpl extends SinkTaskImpl {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestMCSinkTaskImpl.class);
    public TaskHandle taskHandle;
    public String taskId;
    public String connectName;

    private void preOpen(Collection<TopicPartition> partitions) {
        LOGGER.debug("Opening partitions {}", partitions);
        taskHandle.partitionsAssigned(partitions);
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        preOpen(partitions);
        super.open(partitions);
    }

    private void preStart(Map<String, String> map) {
        taskId = map.get("task.id");
        connectName = map.get("connector.name");
        taskHandle = RuntimeHandles.get().connectorHandle(connectName).taskHandle(taskId);
        LOGGER.info("Starting task {}", taskId);
        taskHandle.recordTaskStart();
    }

    @Override
    public void start(Map<String, String> map) {
        preStart(map);
        super.start(map);
    }

    private void prePut(Collection<SinkRecord> collection) {
        for (SinkRecord r : collection) {
            taskHandle.record(r);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        prePut(records);
        super.put(records);
    }

    private void preStop() {
        LOGGER.info("Stopped taskHandle {} task {}", this.getClass().getSimpleName(), taskId);
        taskHandle.recordTaskStop();
        LOGGER.info("Thread({}) Enter STOP", Thread.currentThread().getId());
    }

    @Override
    public void stop() {
        preStop();
        super.stop();
    }

    private void preClose(Collection<TopicPartition> partitions) {
        LOGGER.debug("Thread({}) Enter CLOSE", Thread.currentThread().getId());
        LOGGER.debug("Closing partitions {}", partitions);
        taskHandle.partitionsRevoked(partitions);
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        preClose(partitions);
        super.close(partitions);
    }
}
