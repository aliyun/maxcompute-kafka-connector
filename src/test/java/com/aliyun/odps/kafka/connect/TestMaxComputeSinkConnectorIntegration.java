package com.aliyun.odps.kafka.connect;

import static org.apache.kafka.connect.runtime.WorkerConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.integration.ConnectorHandle;
import org.apache.kafka.connect.integration.RuntimeHandles;
import org.apache.kafka.connect.integration.TaskHandle;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TestMaxComputeSinkConnectorIntegration extends TestConnectorBase {

  // 集成测试类，用于验证Kafka Connect Sink Connector的功能
  // 测试分区分配和消息消费功能
  private static final String currentConnectorName = "connect-integration-test-sink";

  @Test
  public void testEagerConsumerPartitionAssignment() throws Exception {
    setConnectorName(currentConnectorName);
    final String[] topics = {"topic1"};
    final TopicPartition[] topicPartitions = new TopicPartition[3];
    for (int i = 0; i < topics.length; i++) {
      topicPartitions[i] = new TopicPartition(topics[i], 0);
    }

    Map<String, String>
        connectorProps =
        getBaseSinkConnectorProps(String.join(",", topics), null);
    // some specific configuration for the connector
    connectorProps.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    connectorProps.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

    final Set<String> consumedRecordValues = new HashSet<>();
    Consumer<SinkRecord> onPut = record -> {
      Assert.assertTrue("Task received duplicate record from mc-connect",
                        consumedRecordValues.add(Objects.toString(record.value())));
      System.out.println("consumedRecordValues size: " + consumedRecordValues.size());
    };
    ConnectorHandle connector = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
    TaskHandle task = connector.taskHandle(CONNECTOR_NAME + "-0", onPut);
    connectCluster.configureConnector(CONNECTOR_NAME, connectorProps);//
    connectCluster.assertions()
        .assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, TASK_NUM,
                                                     "mc-connector tasks did not start in time!");

    // it should be 0 when no topic is created; task will not be assigned to any partitions
    Assert.assertEquals(0, task.numPartitionsAssigned());

    Set<String> expectedRecordValues = new HashSet<>();
    Set<TopicPartition> expectedAssignment = new HashSet<>();

    connectCluster.kafka().createTopic(topics[0], 1);
    expectedAssignment.add(topicPartitions[0]);
    connectCluster.kafka().produce(topics[0], "topics[0] send record_1");
    expectedRecordValues.add("topics[0] send record_1");

    System.out.println("expectedRecordValues: " + expectedRecordValues.toString());
    waitForCondition(
        () -> expectedRecordValues.equals(consumedRecordValues),
        TASK_CONSUME_TIMEOUT_MS,
        "Task dis not receive records in time");
    System.out.println("consumedRecordValues: " + consumedRecordValues.toString());

    Assert.assertEquals(1, task.timesAssigned(topicPartitions[0]));
    Assert.assertEquals(0, task.timesRevoked(topicPartitions[0]));
    System.out.println("the times of topic0 to be committed" + task.timesCommitted(
        topicPartitions[0]));// 查看分区被提交的次数
    Assert.assertEquals(expectedAssignment, task.assignment());
  }

}
