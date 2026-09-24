package com.aliyun.odps.kafka.connect;

import static com.aliyun.odps.kafka.connect.ConfigParameter.BUFFER_SIZE_KB;
import static com.aliyun.odps.kafka.connect.ConfigParameter.CLIENT_TIMEOUT_MS;
import static com.aliyun.odps.kafka.connect.ConfigParameter.FORMAT;
import static com.aliyun.odps.kafka.connect.ConfigParameter.SKIP_ERROR;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.Assert;
import org.junit.Test;

/**
 * ConfigParameter 与 ConfigDef 的一致性，以及下发到 task 配置时的类型序列化。
 */
public class ConfigParameterTest {

  @Test
  public void enumNamesAreUniqueAndMatchConfigDef() {
    Set<String> names = new HashSet<String>();
    ConfigDef configDef = ConnectorConfig.conf();
    for (ConfigParameter parameter : ConfigParameter.values()) {
      Assert.assertTrue("重复的配置名: " + parameter.getName(), names.add(parameter.getName()));
      Assert.assertTrue("ConfigDef 缺少 " + parameter.getName(),
                        configDef.names().contains(parameter.getName()));
      ConfigDef.ConfigKey key = configDef.configKeys().get(parameter.getName());
      Assert.assertEquals(parameter.getName(), parameter.getType(), key.type);
    }
  }

  @Test
  public void requiredParametersHaveNoDefault() {
    for (ConfigParameter parameter : ConfigParameter.values()) {
      if (parameter == ConfigParameter.MAXCOMPUTE_ENDPOINT
          || parameter == ConfigParameter.MAXCOMPUTE_PROJECT
          || parameter == ConfigParameter.MAXCOMPUTE_TABLE
          || parameter == ConfigParameter.ACCESS_ID
          || parameter == ConfigParameter.ACCESS_KEY) {
        Assert.assertEquals(parameter.getName(), ConfigDef.NO_DEFAULT_VALUE,
                            parameter.getDefaultValue());
      } else {
        Assert.assertNotEquals(parameter.getName() + " 应带默认值",
                               ConfigDef.NO_DEFAULT_VALUE, parameter.getDefaultValue());
      }
    }
  }

  @Test
  public void putSerializesEachSupportedType() {
    Map<String, String> props = new HashMap<String, String>();
    props.put(ConfigParameter.MAXCOMPUTE_ENDPOINT.getName(), "http://service.example.com/api");
    props.put(ConfigParameter.MAXCOMPUTE_PROJECT.getName(), "project_placeholder");
    props.put(ConfigParameter.MAXCOMPUTE_TABLE.getName(), "table_placeholder");
    props.put(ConfigParameter.ACCESS_ID.getName(), "placeholder-id");
    props.put(ConfigParameter.ACCESS_KEY.getName(), "placeholder-key");
    props.put(SKIP_ERROR.getName(), "true");
    props.put(BUFFER_SIZE_KB.getName(), "2048");
    ConnectorConfig config = new ConnectorConfig(props);

    Map<String, String> out = new HashMap<String, String>();
    FORMAT.put(out, config);
    BUFFER_SIZE_KB.put(out, config);
    CLIENT_TIMEOUT_MS.put(out, config);
    SKIP_ERROR.put(out, config);

    Assert.assertEquals("TEXT", out.get(FORMAT.getName()));
    Assert.assertEquals("2048", out.get(BUFFER_SIZE_KB.getName()));
    Assert.assertEquals("39600000", out.get(CLIENT_TIMEOUT_MS.getName()));
    // 布尔值按 "TRUE"/"FALSE" 下发，Java/Kafka 侧 Boolean.valueOf 仍可解析
    Assert.assertEquals("TRUE", out.get(SKIP_ERROR.getName()));
  }
}
