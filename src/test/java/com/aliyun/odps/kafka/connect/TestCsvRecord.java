package com.aliyun.odps.kafka.connect;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.kafka.connect.utils.ConfigHelper;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.integration.ConnectorHandle;
import org.apache.kafka.connect.integration.RuntimeHandles;
import org.apache.kafka.connect.integration.TaskHandle;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.kafka.connect.SinkConnectorTest.TestMCSinkConnector;

public class TestCsvRecord extends TestConnectorBase {

  // 测试json 数据的读取写入
  private static final String currentConnectorName = "csv-test-sink";
  private static final String tableName = "kafka_csv_test_table";
  private static final Map<String, String> config =
          ConfigHelper.getTestConfigFromEnv("ALIYUN", tableName);

  @Before
  public void setUp() throws OdpsException {
    // Create table before test
    config.put("format", "CSV");
    TestTableUtils.createCsvTestTable(config, tableName);
  }

  @AfterClass
  public static void afterClass() throws OdpsException, IOException {
    List<Record> records = TestTableUtils.readTable(config, tableName);
    System.out.println(records);
  }

  @Test
  public void testCsvRead() throws Exception {
    setConnectorName(currentConnectorName);
    final String[] topics = {"topic-json"};
    final TopicPartition[] topicPartitions = new TopicPartition[3];
    for (int i = 0; i < topics.length; i++) {
      topicPartitions[i] = new TopicPartition(topics[i], 0);
    }

    // Use environment variable based configuration
    Map<String, String> connectorProps = getBaseSinkConnectorProps(String.join(",", topics), null);
    // Set the specific table name for this test
    connectorProps.put("table", tableName);
    // Set CSV format
    connectorProps.put("format", "CSV");
    connectorProps.put("use_streaming", "true");
    // some specific configuration for the connector
    connectorProps.put(CONNECTOR_CLASS_CONFIG, TestMCSinkConnector.class.getName());
    connectorProps.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    connectorProps.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    connectorProps.put("value.converter.schemas.enable", "false");
    final Set<String> consumedRecordValues = new HashSet<>();
    Consumer<SinkRecord> onPut = record -> {
      consumedRecordValues.add(record.value().toString());
      System.out.println("consumedRecordValues size: " + consumedRecordValues.size());
    };
    ConnectorHandle connector = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
    TaskHandle task = connector.taskHandle(CONNECTOR_NAME + "-0", onPut);
    connectCluster.configureConnector(CONNECTOR_NAME, connectorProps);
    connectCluster.assertions()
        .assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, TASK_NUM,
                                                     "mc-connector tasks did not start in time!");

    // it should be 0 when no topic is created; task will not be assigned to any partitions
    Assert.assertEquals(0, task.numPartitionsAssigned());

    connectCluster.kafka().createTopic(topics[0], 1);

    // Produce 10 CSV records instead of 1
    for (int i = 0; i < 10; i++) {
      String rawData = "2023042" + i + "," + (3.75 + i/10.0) + "," + (i % 2 == 0 ? "true" : "false") + ",whq" + i + ",2017-11-11 00:00:00.1234,2023-11-11,2021-11-26 00:04:0" + i;
      connectCluster.kafka().produce(topics[0], rawData);
    }

    waitForCondition(
        () -> consumedRecordValues.size() >= 10,
        TASK_CONSUME_TIMEOUT_MS,
        "Task did not receive all records in time");
  }
}
