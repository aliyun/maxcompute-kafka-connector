package com.aliyun.odps.kafka.connect;

import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.connect.integration.RuntimeHandles;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import com.aliyun.odps.kafka.connect.SinkConnectorTest.TestMCSinkConnector;
import com.aliyun.odps.kafka.connect.utils.ConfigHelper;

@Category(IntegrationTest.class)
public class TestConnectorBase {

  // 构建junit kafka-connector测试的基类
  public static final int TASK_NUM = 3;
  public static final int WORKER_NUM = 3;

  public static final long TASK_CONSUME_TIMEOUT_MS = 20_000L;

  public EmbeddedConnectCluster connectCluster;

  public String CONNECTOR_NAME;

  public void setConnectorName(String connectorName) {
    CONNECTOR_NAME = connectorName;
  }

  @Before
  public void setup() throws Exception {
    // configuration kafka connector cluster start setting
    Properties brokerConfig = new Properties();
    brokerConfig.put("auto.create.topics.enable", "false");
    brokerConfig.put("delete.topic.enable", "true");

    Map<String, String> workConfig = new HashMap<>();
    workConfig.put(CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");

    workConfig.put("consumer.max.poll.records", "3000");
    workConfig.put("group.id", "1");
    // create in-memory connect cluster
    connectCluster = new EmbeddedConnectCluster.Builder()
        .name("odps-connect-cluster")
        .numWorkers(WORKER_NUM)
        .workerProps(workConfig)
        .brokerProps(brokerConfig)
        .build();
    connectCluster.start();
    connectCluster.assertions()
        .assertAtLeastNumWorkersAreUp(WORKER_NUM, "connect-cluster worker did not start in time");
    System.out.println("look connector: " + connectCluster.connectors().toString());
  }

  @After
  public void close() {
    RuntimeHandles.get().deleteConnector(CONNECTOR_NAME);
    connectCluster.stop();
  }

  public Map<String, String> getBaseSinkConnectorProps(String topicsList, String confFile) {
    Map<String, String> props;
    if (confFile != null) {
      // Use file-based configuration for backward compatibility
      props = ConfigHelper.getConfigFromFile(confFile);
    } else {
      // Use environment variable based configuration
      props = ConfigHelper.getTestConfigFromEnv("ALIYUN", "kafka_connector");
    }
    props.put(CONNECTOR_CLASS_CONFIG, TestMCSinkConnector.class.getName());
    props.put(TASKS_MAX_CONFIG, String.valueOf(TASK_NUM));
    props.put(TOPICS_CONFIG, topicsList);
    props.put(CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
              RoundRobinAssignor.class.getName());
    // After deleting a topic, offset commits will fail for it; reduce the timeout here so that the test doesn't take forever to proceed past that point
    props.put(CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");
    return props;
  }

  public String getConnectorName() {
    return CONNECTOR_NAME;
  }
}
