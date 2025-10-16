package com.aliyun.odps.kafka.connect;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.aliyun.odps.options.SQLTaskOption;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.integration.ConnectorHandle;
import org.apache.kafka.connect.integration.RuntimeHandles;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.kafka.connect.SinkConnectorTest.TestErrorSinkConnector;
import com.aliyun.odps.kafka.connect.SinkConnectorTest.TestMCSinkConnector;
import com.aliyun.odps.kafka.connect.utils.JsonHandler;
import com.aliyun.odps.task.SQLTask;

public class TestMultiWriter extends TestConnectorBase {

  // 多线程写入测试类
  // 测试多任务并发写入MaxCompute的功能，包括错误处理和偏移量提交
  private static final String currentConnectorName = "flatten-test-sink";

  private String getToken() {
    Instant instant = Instant.now();
    LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    return dateTime.format(formatter);
  }

  @Test
  public void testCommitOffset() throws Exception {
    setConnectorName(currentConnectorName);
    final String topics = "topic-json" + getToken();
    System.out.println("topic: ===>" + topics);
    final int task_num = 3;
    int partition = 4;
    Map<String, String>
        connectorProps =
        getBaseSinkConnectorProps(String.join(",", topics), null);
    // some specific configuration for the connector
    connectorProps.put(CONNECTOR_CLASS_CONFIG, TestMCSinkConnector.class.getName());
    connectorProps.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    connectorProps.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    connectorProps.put("value.converter.schemas.enable", "false");
    connectorProps.put("tasks.max", Integer.toString(task_num));
    connectorProps.put(MaxComputeSinkConnectorConfig.BaseParameter.RECORD_BATCH_SIZE.getName(),
                       "1000");
    // 先生成数据
    long totalMsg = 1000;
    mockDate(topics, totalMsg, connectCluster, "flatten.json", partition);
    // 创建kafka-connect cluster 和相关服务
    ConnectorHandle connector = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
    connector.taskHandle(CONNECTOR_NAME + "-0", null);
    connectCluster.configureConnector(CONNECTOR_NAME, connectorProps);//
    connectCluster.assertions()
        .assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, 1,
                                                     "mc-connector tasks did not start in time!");
    String table = connectorProps.get("table");
    Assert.assertEquals(totalMsg, getTotalMessage(600000, topics, table, totalMsg,
                                                  getOdpsFromProps(connectorProps)));

  }

  @Test
  @Ignore("Not Implement yet")
  public void testRunTimeErrorSkip() throws Exception {
    // 测试运行过程中出现的数据错误, 写入Kafka-队列runtimeError消息队列;
    // 在拉取到数据的过程中，使用钩子函数，随机破坏一些数据的格式，造成数据转换过程中存在失败，并且在特定的Kafka消息队列中能够看到对应的出错数据
    setConnectorName(currentConnectorName);
    final String topics = "topic-json" + getToken();
    System.out.println("topic: ===>" + topics);
    final int task_num = 1;
    int partition = 1;
    Map<String, String>
        connectorProps =
        getBaseSinkConnectorProps(String.join(",", topics), null);
    // some specific configuration for the connector
    connectorProps.put(CONNECTOR_CLASS_CONFIG, TestErrorSinkConnector.class.getName());
    connectorProps.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    connectorProps.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    connectorProps.put("value.converter.schemas.enable", "false");
    connectorProps.put(MaxComputeSinkConnectorConfig.BaseParameter.FORMAT.getName(), "FLATTEN");
    connectorProps.put(MaxComputeSinkConnectorConfig.BaseParameter.MODE.getName(), "VALUE");
    connectorProps.put(MaxComputeSinkConnectorConfig.BaseParameter.POOL_SIZE.getName(), "8");
    connectorProps.put("tasks.max", Integer.toString(task_num));
    connectorProps.put(MaxComputeSinkConnectorConfig.BaseParameter.RECORD_BATCH_SIZE.getName(),
                       "1000");
    connectorProps.put(MaxComputeSinkConnectorConfig.BaseParameter.SKIP_ERROR.getName(), "true");
    // 先生成数据
    long totalMsg = 2000;
    mockDate(topics, totalMsg, connectCluster, "flatten.json", partition);
    // 创建kafka-connect cluster 和相关服务
    ConnectorHandle connector = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
    connector.taskHandle(CONNECTOR_NAME + "-0", null);
    connectCluster.configureConnector(CONNECTOR_NAME, connectorProps);//
    connectCluster.assertions()
        .assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, 1,
                                                     "mc-connector tasks did not start in time!");

    // it should be 0 when no topic is created; task will not be assigned to any partitions
    // 查询 odps 来确定是否有数据持续写入来判断是否停止
    String table = connectorProps.get("table");
    Assert.assertEquals(totalMsg / 2, getTotalMessage(600000, topics, table, totalMsg / 2,
                                                      getOdpsFromProps(connectorProps)));
  }

  @Test
  @Ignore("Not Implement yet")
  public void testRunTimeErrorFail() throws Exception {
    // 测试运行过程中出现的数据错误, 写入Kafka-队列runtimeError消息队列;
    // 在拉取到数据的过程中，使用钩子函数，随机破坏一些数据的格式，造成数据转换过程中存在失败，并且在特定的Kafka消息队列中能够看到对应的出错数据
    setConnectorName(currentConnectorName);
    final String topics = "topic-json" + getToken();
    System.out.println("topic: ===>" + topics);
    final int task_num = 3;
    int partition = 4;
    Map<String, String>
        connectorProps =
        getBaseSinkConnectorProps(String.join(",", topics), null);
    // some specific configuration for the connector
    connectorProps.put(CONNECTOR_CLASS_CONFIG, TestErrorSinkConnector.class.getName());
    connectorProps.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    connectorProps.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    connectorProps.put("value.converter.schemas.enable", "false");
    connectorProps.put(MaxComputeSinkConnectorConfig.BaseParameter.FORMAT.getName(), "FLATTEN");
    connectorProps.put(MaxComputeSinkConnectorConfig.BaseParameter.MODE.getName(), "VALUE");
    connectorProps.put(MaxComputeSinkConnectorConfig.BaseParameter.POOL_SIZE.getName(), "8");
    connectorProps.put("tasks.max", Integer.toString(task_num));
    connectorProps.put(MaxComputeSinkConnectorConfig.BaseParameter.RECORD_BATCH_SIZE.getName(),
                       "1000");
    connectorProps.put(MaxComputeSinkConnectorConfig.BaseParameter.SKIP_ERROR.getName(), "false");
    // 先生成数据
    long totalMsg = 2000;
    mockDate(topics, totalMsg, connectCluster, "flatten.json", partition);
    // 创建kafka-connect cluster 和相关服务
    ConnectorHandle connector = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
    connector.taskHandle(CONNECTOR_NAME + "-0", null);
    connectCluster.configureConnector(CONNECTOR_NAME, connectorProps);//
    connectCluster.assertions()
        .assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, 1,
                                                     "mc-connector tasks did not start in time!");

    // it should be 0 when no topic is created; task will not be assigned to any partitions
    // 查询 odps 来确定是否有数据持续写入来判断是否停止
    String table = connectorProps.get("table");
    Assert.assertTrue(totalMsg / 2 > getTotalMessage(100000, topics, table, totalMsg / 2,
                                                     getOdpsFromProps(connectorProps)));
  }

  private void mockDate(String topics, long totalMsg, EmbeddedConnectCluster connectCluster,
                        String dataPath, int partition)
      throws ExecutionException, InterruptedException, TimeoutException {
    connectCluster.kafka().createTopic(topics, partition);
    String rawData = JsonHandler.readJson(dataPath);
    for (int i = 0; i < totalMsg; i++) {
      connectCluster.kafka().produce(topics, rawData);
    }
    ConsumerRecords<byte[], byte[]>
        records =
        this.connectCluster.kafka().consumeAll(10000, null, null, topics);
    Assert.assertEquals(totalMsg, records.count());
  }

  private Odps getOdpsFromProps(Map<String, String> connectorProps) {
    Account
        account =
        new AliyunAccount(connectorProps.get("access_id"), connectorProps.get("access_key"));
    Odps odps = new Odps(account);
    String odpsUrl = connectorProps.get("endpoint");
    String project = connectorProps.get("project");
    odps.setEndpoint(odpsUrl);
    odps.setDefaultProject(project);
    return odps;
  }

  private long getTotalMessage(long timeout, String topics, String table, long totalMsg,
                               Odps odps) {
    // it should be 0 when no topic is created; task will not be assigned to any partitions
    // 查询 odps 来确定是否有数据持续写入来判断是否停止
    String sql = "select count(*) as total from " + table + " where topic=\"" + topics + "\";";
    Instance instance;
    long pre = 0L;
//    int retryTimes = 20;
    Time time = new SystemTime();
    long now = time.milliseconds();
    long start = now;
    while (pre < totalMsg) {
      try {
        Map<String, String> hints = new HashMap<>();
        hints.put("odps.sql.allow.fullscan", "true");
        instance = SQLTask.run(odps, odps.getDefaultProject(),  sql, hints, null);
        instance.waitForSuccess();
        ResultSet res = SQLTask.getResultSet(instance);
        Long okRecords = res.next().getBigint("total");
        if (okRecords > pre) {
          pre = okRecords;
        }
        now = time.milliseconds();
        if (now >= start + timeout) {
          System.out.println("Time out in connectCluster");
          break;
        }
      } catch (OdpsException | IOException e) {
        System.out.println("error in test multi-writer " + e);
        break;
      }
    }
    System.out.println("total commit size :" + pre);
    return pre;
  }

  @Test
  @Ignore("Need Kakfa Brocker")
  public void testRunTimeKafka() throws ExecutionException, InterruptedException, TimeoutException {
    // 测试运行时候写入错误的Kafka消息队列中
    String runTimeTopic = "runtime_error";
    String bootStrapServer = "http://11.158.160.203:9093";
    setConnectorName(currentConnectorName);
    final String topics = "topic-json" + getToken();
    final int task_num = 3;
    int partition = 1;
    Map<String, String>
        connectorProps =
        getBaseSinkConnectorProps(String.join(",", topics), null);
    // some specific configuration for the connector
    connectorProps.put(CONNECTOR_CLASS_CONFIG, TestErrorSinkConnector.class.getName());
    connectorProps.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    connectorProps.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    connectorProps.put("value.converter.schemas.enable", "false");
    connectorProps.put(MaxComputeSinkConnectorConfig.BaseParameter.FORMAT.getName(), "FLATTEN");
    connectorProps.put(MaxComputeSinkConnectorConfig.BaseParameter.MODE.getName(), "VALUE");
    connectorProps.put(MaxComputeSinkConnectorConfig.BaseParameter.POOL_SIZE.getName(), "8");
    connectorProps.put("tasks.max", Integer.toString(task_num));
    connectorProps.put(MaxComputeSinkConnectorConfig.BaseParameter.RECORD_BATCH_SIZE.getName(),
                       "1000");
    connectorProps.put(MaxComputeSinkConnectorConfig.BaseParameter.SKIP_ERROR.getName(), "false");
    connectorProps.put(
        MaxComputeSinkConnectorConfig.BaseParameter.RUNTIME_ERROR_TOPIC_NAME.getName(),
        runTimeTopic);
    connectorProps.put(
        MaxComputeSinkConnectorConfig.BaseParameter.RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS.getName(),
        bootStrapServer);
    // 先生成数据
    long totalMsg = 200;
    mockDate(topics, totalMsg, connectCluster, "flatten.json", partition);
    // 创建kafka-connect cluster 和相关服务
    ConnectorHandle connector = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
    connector.taskHandle(CONNECTOR_NAME + "-0", null);
    connectCluster.configureConnector(CONNECTOR_NAME, connectorProps);//
    connectCluster.assertions()
        .assertConnectorAndAtLeastNumTasksAreRunning(CONNECTOR_NAME, 1,
                                                     "mc-connector tasks did not start in time!");

    // it should be 0 when no topic is created; task will not be assigned to any partitions
    // 查询 odps 来确定是否有数据持续写入来判断是否停止
    String table = connectorProps.get("table");
    Assert.assertEquals(totalMsg / 2, getTotalMessage(100000, topics, table, totalMsg / 2,
                                                      getOdpsFromProps(connectorProps)));
  }
}
