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

import static com.aliyun.odps.kafka.connect.ConfigParameter.*;
import com.aliyun.odps.kafka.connect.utils.OdpsUtils;

/**
 * Connector entry class
 *
 * don't rename this class
 */
public class MaxComputeSinkConnector extends SinkConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(MaxComputeSinkConnector.class);
  protected ConnectorConfig config;

  @Override
  public void start(Map<String, String> map) {
    config = new ConnectorConfig(map);

    LOGGER.info("Starting MaxCompute sink connector");
    for (Entry<String, String> entry : map.entrySet()) {
      LOGGER.info(entry.getKey() + ": " + entry.getValue());
    }

    Odps odps = OdpsUtils.getOdps(config);

    try {
      odps.projects().exists(MAXCOMPUTE_PROJECT.getString(config));
      odps.tables().exists(MAXCOMPUTE_TABLE.getString(config));
    } catch (OdpsException e) {
      throw new IllegalArgumentException("Cannot find configured MaxCompute project or table");
    }

    // TODO: validate table schema
    LOGGER.info("Connect to MaxCompute successfully!");
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SinkTaskImpl.class;
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
    for(ConfigParameter p:ConfigParameter.values()){
      p.put(taskConfig,config);
    }
    return taskConfig;
  }

  @Override
  public void stop() {
    // do nothing
  }

  @Override
  public ConfigDef config() {
    return ConnectorConfig.conf();
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }
}
