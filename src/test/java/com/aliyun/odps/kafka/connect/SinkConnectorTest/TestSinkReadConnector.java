package com.aliyun.odps.kafka.connect.SinkConnectorTest;

import org.apache.kafka.connect.connector.Task;


public class TestSinkReadConnector extends TestMCSinkConnector {

  @Override
  public Class<? extends Task> taskClass() {
    return TestSinkReadTask.class;
  }

}
