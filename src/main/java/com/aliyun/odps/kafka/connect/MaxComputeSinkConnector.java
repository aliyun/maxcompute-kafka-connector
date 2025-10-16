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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.kafka.connect.MaxComputeSinkConnectorConfig.BaseParameter;
import com.aliyun.odps.kafka.connect.utils.OdpsUtils;

/**
 * Connector entry class
 */
public class MaxComputeSinkConnector extends SinkConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(MaxComputeSinkConnector.class);
  protected MaxComputeSinkConnectorConfig config;

  @Override
  public void start(Map<String, String> map) {
    config = new MaxComputeSinkConnectorConfig(map);

    LOGGER.info("Starting MaxCompute sink connector");
    for (Entry<String, String> entry : map.entrySet()) {
      LOGGER.info(entry.getKey() + ": " + entry.getValue());
    }

    Odps odps = OdpsUtils.getOdps(config);

    try {
      odps.projects().exists(config.getString(BaseParameter.MAXCOMPUTE_PROJECT.getName()));
      odps.tables().exists(config.getString(BaseParameter.MAXCOMPUTE_TABLE.getName()));
    } catch (OdpsException e) {
      throw new IllegalArgumentException("Cannot find configured MaxCompute project or table");
    }

    // TODO: validate table schema
    LOGGER.info("Connect to MaxCompute successfully!");
  }

  @Override
  public Class<? extends Task> taskClass() {
    return MaxComputeSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> taskConfigs = new ArrayList<>();
    Map<String, String> taskConfig = createTaskConfig();
    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(new HashMap<>(taskConfig));
    }
    return taskConfigs;
  }

  private Map<String, String> createTaskConfig() {
    Map<String, String> taskConfig = new HashMap<>();
    taskConfig.put(BaseParameter.ACCESS_ID.getName(),
                   config.getString(BaseParameter.ACCESS_ID.getName()));

    taskConfig.put(BaseParameter.TUNNEL_ENDPOINT.getName(),
                   config.getString(BaseParameter.TUNNEL_ENDPOINT.getName()));

    taskConfig.put(BaseParameter.ACCESS_KEY.getName(),
                   config.getString(BaseParameter.ACCESS_KEY.getName()));

    taskConfig.put(BaseParameter.ACCOUNT_ID.getName(),
                   config.getString(BaseParameter.ACCOUNT_ID.getName()));

    taskConfig.put(BaseParameter.REGION_ID.getName(),
                   config.getString(BaseParameter.REGION_ID.getName()));

    taskConfig.put(BaseParameter.STS_ENDPOINT.getName(),
                   config.getString(BaseParameter.STS_ENDPOINT.getName()));

    taskConfig.put(BaseParameter.ROLE_NAME.getName(),
                   config.getString(BaseParameter.ROLE_NAME.getName()));

    taskConfig.put(BaseParameter.ACCOUNT_TYPE.getName(),
                   config.getString(BaseParameter.ACCOUNT_TYPE.getName()));

    taskConfig.put(BaseParameter.CLIENT_TIMEOUT_MS.getName(),
                   String.valueOf(config.getLong(BaseParameter.CLIENT_TIMEOUT_MS.getName())));

    taskConfig.put(BaseParameter.MAXCOMPUTE_PROJECT.getName(),
                   config.getString(BaseParameter.MAXCOMPUTE_PROJECT.getName()));

    taskConfig.put(BaseParameter.MAXCOMPUTE_SCHEMA.getName(),
                   config.getString(BaseParameter.MAXCOMPUTE_SCHEMA.getName()));

    taskConfig.put(BaseParameter.MAXCOMPUTE_ENDPOINT.getName(),
                   config.getString(BaseParameter.MAXCOMPUTE_ENDPOINT.getName()));

    taskConfig.put(BaseParameter.MAXCOMPUTE_TABLE.getName(),
                   config.getString(BaseParameter.MAXCOMPUTE_TABLE.getName()));

    taskConfig.put(BaseParameter.RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS.getName(),
                   config.getString(BaseParameter.RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS.getName()));

    taskConfig.put(BaseParameter.RUNTIME_ERROR_TOPIC_NAME.getName(),
                   config.getString(BaseParameter.RUNTIME_ERROR_TOPIC_NAME.getName()));

    taskConfig.put(BaseParameter.FORMAT.getName(),
                   config.getString(BaseParameter.FORMAT.getName()));

    taskConfig.put(BaseParameter.MODE.getName(), config.getString(BaseParameter.MODE.getName()));

    taskConfig.put(BaseParameter.PARTITION_WINDOW_TYPE.getName(),
                   config.getString(BaseParameter.PARTITION_WINDOW_TYPE.getName()));

    taskConfig.put(BaseParameter.USE_NEW_PARTITION_FORMAT.getName(),
            config.getBoolean(BaseParameter.USE_NEW_PARTITION_FORMAT.getName())? "TRUE": "FALSE");

    taskConfig.put(BaseParameter.TIME_ZONE.getName(),
                   config.getString(BaseParameter.TIME_ZONE.getName()));

    taskConfig.put(BaseParameter.USE_STREAM_TUNNEL.getName(),
                   config.getBoolean(BaseParameter.USE_STREAM_TUNNEL.getName()) ? "TRUE" : "FALSE");

    taskConfig.put(BaseParameter.BUFFER_SIZE_KB.getName(),
                   Integer.toString(config.getInt(BaseParameter.BUFFER_SIZE_KB.getName())));

    taskConfig.put(BaseParameter.FAIL_RETRY_TIMES.getName(),
                   Integer.toString(config.getInt(BaseParameter.FAIL_RETRY_TIMES.getName())));

    taskConfig.put(BaseParameter.POOL_SIZE.getName(),
                   Integer.toString(config.getInt(BaseParameter.POOL_SIZE.getName())));

    taskConfig.put(BaseParameter.RECORD_BATCH_SIZE.getName(),
                   Integer.toString(config.getInt(BaseParameter.RECORD_BATCH_SIZE.getName())));

    taskConfig.put(BaseParameter.SKIP_ERROR.getName(),
                   config.getBoolean(BaseParameter.SKIP_ERROR.getName()) ? "TRUE" : "FALSE");

    return taskConfig;
  }

  @Override
  public void stop() {
    // do nothing
  }

  @Override
  public ConfigDef config() {
    return MaxComputeSinkConnectorConfig.conf();
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }
}
