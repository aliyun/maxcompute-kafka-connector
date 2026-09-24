package com.aliyun.odps.kafka.connect;

import static com.aliyun.odps.kafka.connect.ConfigParameter.ACCOUNT_TYPE;
import static com.aliyun.odps.kafka.connect.ConfigParameter.BUFFER_SIZE_KB;
import static com.aliyun.odps.kafka.connect.ConfigParameter.CLIENT_TIMEOUT_MS;
import static com.aliyun.odps.kafka.connect.ConfigParameter.CSV_DELIMITER;
import static com.aliyun.odps.kafka.connect.ConfigParameter.FORMAT;
import static com.aliyun.odps.kafka.connect.ConfigParameter.MAXCOMPUTE_ENDPOINT;
import static com.aliyun.odps.kafka.connect.ConfigParameter.MAXCOMPUTE_PROJECT;
import static com.aliyun.odps.kafka.connect.ConfigParameter.MAXCOMPUTE_SCHEMA;
import static com.aliyun.odps.kafka.connect.ConfigParameter.MAXCOMPUTE_TABLE;
import static com.aliyun.odps.kafka.connect.ConfigParameter.MODE;
import static com.aliyun.odps.kafka.connect.ConfigParameter.PARTITION_WINDOW_TYPE;
import static com.aliyun.odps.kafka.connect.ConfigParameter.SKIP_ERROR;
import static com.aliyun.odps.kafka.connect.ConfigParameter.TUNNEL_ENDPOINT;
import static com.aliyun.odps.kafka.connect.ConfigParameter.USE_NEW_PARTITION_FORMAT;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Assert;
import org.junit.Test;

/**
 * 配置解析与默认值回归：不需要任何云凭据，属于 PR 门禁。
 */
public class ConnectorConfigTest {

  private static Map<String, String> requiredOnly() {
    Map<String, String> props = new HashMap<String, String>();
    props.put(MAXCOMPUTE_ENDPOINT.getName(), "http://service.example.com/api");
    props.put(MAXCOMPUTE_PROJECT.getName(), "project_placeholder");
    props.put(MAXCOMPUTE_TABLE.getName(), "table_placeholder");
    props.put(ConfigParameter.ACCESS_ID.getName(), "placeholder-id");
    props.put(ConfigParameter.ACCESS_KEY.getName(), "placeholder-key");
    return props;
  }

  @Test
  public void documentedDefaultsAreApplied() {
    ConnectorConfig config = new ConnectorConfig(requiredOnly());
    Assert.assertEquals("TEXT", FORMAT.getString(config));
    Assert.assertEquals("DEFAULT", MODE.getString(config));
    Assert.assertEquals(",", CSV_DELIMITER.getString(config));
    Assert.assertEquals("HOUR", PARTITION_WINDOW_TYPE.getString(config));
    Assert.assertEquals("ALIYUN", ACCOUNT_TYPE.getString(config));
    Assert.assertEquals("", MAXCOMPUTE_SCHEMA.getString(config));
    Assert.assertEquals("", TUNNEL_ENDPOINT.getString(config));
    Assert.assertEquals(64 * 1024, BUFFER_SIZE_KB.getInt(config));
    Assert.assertEquals(Long.valueOf(11L * 60 * 60 * 1000),
                        Long.valueOf(config.getLong(CLIENT_TIMEOUT_MS.getName())));
    Assert.assertFalse(USE_NEW_PARTITION_FORMAT.getBoolean(config));
    Assert.assertFalse(SKIP_ERROR.getBoolean(config));
    Assert.assertEquals(TimeZone.getDefault().getID(),
                        config.getString(ConfigParameter.TIME_ZONE.getName()));
  }

  @Test
  public void explicitValuesWinOverDefaults() {
    Map<String, String> props = requiredOnly();
    props.put(FORMAT.getName(), "CSV");
    props.put(CSV_DELIMITER.getName(), "\\t");
    props.put(MODE.getName(), "KEY");
    props.put(BUFFER_SIZE_KB.getName(), "1024");
    props.put(USE_NEW_PARTITION_FORMAT.getName(), "true");
    ConnectorConfig config = new ConnectorConfig(props);
    Assert.assertEquals("CSV", FORMAT.getString(config));
    Assert.assertEquals("\\t", CSV_DELIMITER.getString(config));
    Assert.assertEquals("KEY", MODE.getString(config));
    Assert.assertEquals(1024, BUFFER_SIZE_KB.getInt(config));
    Assert.assertTrue(USE_NEW_PARTITION_FORMAT.getBoolean(config));
  }

  @Test
  public void nonNumericBufferSizeIsRejected() {
    Map<String, String> props = requiredOnly();
    props.put(BUFFER_SIZE_KB.getName(), "64MB");
    try {
      new ConnectorConfig(props);
      Assert.fail("buffer_size_kb 配成非数字应报错，而不是回落到默认值");
    } catch (ConfigException expected) {
      Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("buffer_size_kb"));
    }
  }

  @Test
  public void missingRequiredConfigIsRejected() {
    Map<String, String> props = requiredOnly();
    props.remove(MAXCOMPUTE_ENDPOINT.getName());
    try {
      new ConnectorConfig(props);
      Assert.fail("endpoint 没有默认值，缺失时必须报错");
    } catch (ConfigException expected) {
      Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("endpoint"));
    }
  }

  @Test
  public void connectorExposesTheSameConfigDef() {
    ConfigDef fromConnector = new MaxComputeSinkConnector().config();
    ConfigDef fromConfig = ConnectorConfig.conf();
    Assert.assertEquals(fromConfig.names(), fromConnector.names());
    for (ConfigParameter parameter : ConfigParameter.values()) {
      Assert.assertTrue(parameter.getName(), fromConnector.names().contains(parameter.getName()));
    }
  }

  @Test
  public void taskConfigsCarryEveryParameterAndAreIndependentCopies() {
    MaxComputeSinkConnector connector = new MaxComputeSinkConnector();
    // 跳过 start()：它要访问服务。这里只需要验证 connector -> task 的配置透传契约。
    connector.config = new ConnectorConfig(requiredOnly());

    List<Map<String, String>> taskConfigs = connector.taskConfigs(2);
    Assert.assertEquals(2, taskConfigs.size());
    for (Map<String, String> taskConfig : taskConfigs) {
      for (ConfigParameter parameter : ConfigParameter.values()) {
        Assert.assertTrue(parameter.getName() + " 必须出现在 task config 里",
                          taskConfig.containsKey(parameter.getName()));
      }
      Assert.assertEquals("65536", taskConfig.get(BUFFER_SIZE_KB.getName()));
      Assert.assertEquals("FALSE", taskConfig.get(USE_NEW_PARTITION_FORMAT.getName()));
      Assert.assertEquals("39600000", taskConfig.get(CLIENT_TIMEOUT_MS.getName()));
    }
    taskConfigs.get(0).put(FORMAT.getName(), "CSV");
    Assert.assertEquals("TEXT", taskConfigs.get(1).get(FORMAT.getName()));
  }
}
