package com.aliyun.odps.kafka.connect.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ConfigHelper {

  public static Map<String, String> getConfigFromFile(String confFile) {
    Map<String, String> props = new HashMap<>();
    // here put your specific connector class name
    // default class name use MonitorableSinkConnector in kafka source code test
    // necessary config for odps
    InputStream inputStream = ConfigHelper.class.getClassLoader().getResourceAsStream(confFile);
    Properties fixedProperties = new Properties();
    try {
      fixedProperties.load(inputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Set<String> propsList = fixedProperties.stringPropertyNames();
    for (String item : propsList) {
      props.put(item, fixedProperties.getProperty(item));
    }
    return props;
  }

  /**
   * Gets configuration from environment variables with fallback to default values
   *
   * @param accountType The account type (ALIYUN or STS)
   * @param tableName   The table name to use
   * @return Configuration map
   */
  public static Map<String, String> getTestConfigFromEnv(String accountType, String tableName) {
    Map<String, String> props = new HashMap<>();

    // Get sensitive information from environment variables
    String accessId = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
    String accessKey = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
    String endpoint = System.getenv("odps_endpoint");
    String project = System.getenv("MAXCOMPUTE_PROJECT");

    if (accessId != null) {
      props.put("access_id", accessId);
    }
    if (accessKey != null) {
      props.put("access_key", accessKey);
    }
    if (endpoint != null) {
      props.put("endpoint", endpoint);
    }
    if (project != null) {
      props.put("project", project);
    }

    // Set account type
    props.put("account_type", accountType != null ? accountType : "ALIYUN");

    // Set table name
    if (tableName != null) {
      props.put("table", tableName);
    }

    // Set default values for other properties
    props.put("partition_window_type", "HOUR");
    props.put("mode", "VALUE");

    return props;
  }

}
