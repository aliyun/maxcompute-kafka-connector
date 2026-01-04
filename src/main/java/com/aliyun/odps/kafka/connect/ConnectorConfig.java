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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class ConnectorConfig extends AbstractConfig {

    private final Map<String, String> configMap;

    public ConnectorConfig(Map<String, String> parsedConfig) {
        super(conf(), parsedConfig);
        this.configMap = parsedConfig;
    }

    public static ConfigDef conf() {
        ConfigDef configDef = new ConfigDef();
        for (ConfigParameter p : ConfigParameter.values()) {
            configDef.define(p.getName(), p.getType(), p.getDefaultValue(), p.getImportance(), p.getDoc());
        }
        return configDef;
    }

    public Map<String, String> getConfigMap() {
        return configMap;
    }
}
