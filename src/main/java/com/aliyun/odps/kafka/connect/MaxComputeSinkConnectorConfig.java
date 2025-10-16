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

import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import com.aliyun.odps.account.Account;

public class MaxComputeSinkConnectorConfig extends AbstractConfig {

  private final Map<String, String> configMap;

  public enum BaseParameter {
    MAXCOMPUTE_ENDPOINT("endpoint"),
    MAXCOMPUTE_PROJECT("project"),
    MAXCOMPUTE_SCHEMA("schema"),
    MAXCOMPUTE_TABLE("table"),
    TUNNEL_ENDPOINT("tunnel_endpoint"),
    ACCESS_ID("access_id"),
    ACCESS_KEY("access_key"),
    ACCOUNT_ID("account_id"),
    REGION_ID("region_id"),
    STS_ENDPOINT("sts.endpoint"),
    ROLE_NAME("role_name"),
    ACCOUNT_TYPE("account_type"),
    CLIENT_TIMEOUT_MS("client_timeout_ms"),
    RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS("runtime.error.topic.bootstrap.servers"),
    RUNTIME_ERROR_TOPIC_NAME("runtime.error.topic.name"),
    FORMAT("format"),
    MODE("mode"),
    PARTITION_WINDOW_TYPE("partition_window_type"),
    USE_NEW_PARTITION_FORMAT("use_new_partition_format"),
    TIME_ZONE("time_zone"),
    USE_STREAM_TUNNEL("use_streaming"),
    BUFFER_SIZE_KB("buffer_size_kb"),
    FAIL_RETRY_TIMES("retry_times"),
    POOL_SIZE("sink_pool_size"),
    RECORD_BATCH_SIZE("record_batch_size"),
    DEFAULT_STS_ENDPOINT("sts.aliyuncs.com"),
    SKIP_ERROR("skip_error");

    private final String name;

    BaseParameter(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  //  default value
  public static final long DEFAULT_CLIENT_TIME_OUT_MS = 11 * 60 * 60 * 1000; // 11 hour

  public MaxComputeSinkConnectorConfig(Map<String, String> parsedConfig) {
    super(conf(), parsedConfig);
    this.configMap = parsedConfig;
  }

  public static ConfigDef conf() {
    ConfigDef configDef = new ConfigDef();
    configDef
      .define(BaseParameter.POOL_SIZE.getName(),
                Type.INT,
                Runtime.getRuntime().availableProcessors(),
                Importance.MEDIUM,
                "MaxCompute sink pool size")
      .define(BaseParameter.RECORD_BATCH_SIZE.getName(),
                Type.INT,
                8000,
                Importance.MEDIUM, "max record size for single writer-thread")
      .define(BaseParameter.MAXCOMPUTE_ENDPOINT.getName(),
                Type.STRING,
                Importance.HIGH,
                "MaxCompute endpoint")
      .define(BaseParameter.MAXCOMPUTE_PROJECT.getName(),
                Type.STRING,
                Importance.HIGH,
                "MaxCompute project")
      .define(BaseParameter.MAXCOMPUTE_SCHEMA.getName(),
                Type.STRING,
                "",
                Importance.MEDIUM,
                "MaxCompute schema")
      .define(BaseParameter.MAXCOMPUTE_TABLE.getName(),
                Type.STRING,
                Importance.HIGH,
                "MaxCompute table")
      .define(BaseParameter.TUNNEL_ENDPOINT.getName(),
                Type.STRING,
                "",
                Importance.MEDIUM,
                "Tunnel endpoint")
      .define(BaseParameter.ACCESS_ID.getName(),
                Type.STRING,
                Importance.HIGH,
                "Aliyun access ID")
      .define(BaseParameter.ACCESS_KEY.getName(),
                Type.STRING,
                Importance.HIGH,
                "Aliyun access key")
      .define(BaseParameter.ACCOUNT_ID.getName(),
                Type.STRING,
                "",
                Importance.HIGH,
                "Account id for STS")
      .define(BaseParameter.REGION_ID.getName(),
                Type.STRING,
                "",
                Importance.HIGH,
                "Region id for STS")
      .define(BaseParameter.STS_ENDPOINT.getName(),
                ConfigDef.Type.STRING,
                BaseParameter.DEFAULT_STS_ENDPOINT.getName(),
                ConfigDef.Importance.HIGH,
                "Sts endpoint")
      .define(BaseParameter.ROLE_NAME.getName(),
                Type.STRING,
                "",
                Importance.HIGH,
                "Role name for STS")
      .define(BaseParameter.ACCOUNT_TYPE.getName(),
                Type.STRING,
                Account.AccountProvider.ALIYUN.toString(),
                Importance.HIGH,
                "Account type: STS Authorization (STS) or Primary Aliyun Account (ALIYUN)")
      .define(BaseParameter.CLIENT_TIMEOUT_MS.getName(),
                Type.LONG,
                DEFAULT_CLIENT_TIME_OUT_MS,
                Importance.MEDIUM,
                "STS token time out")
      .define(BaseParameter.RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS.getName(),
                Type.STRING,
                "",
                Importance.MEDIUM,
                "Bootstrap servers")
      .define(BaseParameter.RUNTIME_ERROR_TOPIC_NAME.getName(),
                Type.STRING,
                "",
                Importance.MEDIUM,
                "Error topic name")
      .define(BaseParameter.FORMAT.getName(),
                Type.STRING,
                "TEXT",
                Importance.HIGH,
                "Input format, could be TEXT or CSV")
      .define(BaseParameter.MODE.getName(),
                Type.STRING,
                "DEFAULT",
                Importance.HIGH,
                "Mode, could be default, key, or value")
      .define(BaseParameter.PARTITION_WINDOW_TYPE.getName(),
                Type.STRING,
                "HOUR",
                Importance.HIGH,
                "Partition window type, could be DAY, HOUR")
      .define(BaseParameter.USE_NEW_PARTITION_FORMAT.getName(),
              Type.BOOLEAN,
              Boolean.FALSE,
              Importance.HIGH,
              "use new partition format,if true then yyyy-MM-dd else MM-dd-yyyy")

      .define(BaseParameter.TIME_ZONE.getName(),
                Type.STRING,
                TimeZone.getDefault().getID(),
                Importance.HIGH,
                "Timezone")
      .define(BaseParameter.USE_STREAM_TUNNEL.getName(),
                Type.BOOLEAN,
                false,
                Importance.LOW,
                "use streaming tunnel instead of batch tunnel")
      .define(BaseParameter.BUFFER_SIZE_KB.getName(),
                Type.INT,
                64 * 1024,
                Importance.MEDIUM,
                "internal buffer size per odps partition in KB, default 64MB")
      .define(BaseParameter.FAIL_RETRY_TIMES.getName(),
                Type.INT,
                3,
                Importance.MEDIUM,
              "retry times on flush failure. default 3 times. if invalid value provided, will fallback to default "
              + "value.")
      .define(BaseParameter.SKIP_ERROR.getName(),
                Type.BOOLEAN,
                false,
                Importance.LOW,
                "the task policy when internal errors happen, SKIP or EXIT");
    return configDef;
  }

  public Map<String, String> getConfigMap() {
    return configMap;
  }
}
