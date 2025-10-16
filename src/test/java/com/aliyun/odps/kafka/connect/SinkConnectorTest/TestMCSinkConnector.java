package com.aliyun.odps.kafka.connect.SinkConnectorTest;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.integration.ConnectorHandle;
import org.apache.kafka.connect.integration.RuntimeHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.kafka.connect.MaxComputeSinkConnector;
import com.aliyun.odps.kafka.connect.MaxComputeSinkConnectorConfig;

public class TestMCSinkConnector extends MaxComputeSinkConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(MaxComputeSinkConnector.class);
  private ConnectorHandle connectorHandle;
  private String connectorName;
  private Map<String, String> commonConfigs;

  @Override
  public void start(Map<String, String> props) {
    connectorHandle = RuntimeHandles.get().connectorHandle(props.get("name"));
    connectorName = props.get("name");
    commonConfigs = props;
    LOGGER.info("Starting connectorHandle {}", props.get("name"));
    connectorHandle.recordConnectorStart();
    config = new MaxComputeSinkConnectorConfig(props);
    LOGGER.info("Starting MaxCompute sink connector");
    for (Map.Entry<String, String> entry : props.entrySet()) {
      LOGGER.info(entry.getKey() + ": " + entry.getValue());
    }
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> configs = super.taskConfigs(maxTasks);
    for (int i = 0; i < maxTasks; i++) {
      configs.get(i).put("connector.name", connectorName);
      configs.get(i).put("task.id", connectorName + "-" + i);
    }
    return configs;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return TestMCSinkTask.class;
  }

  @Override
  public void stop() {
    super.stop();
    LOGGER.info("Stopped-connector: {} {}", this.getClass().getSimpleName(), connectorName);
    connectorHandle.recordConnectorStop();
  }
}
