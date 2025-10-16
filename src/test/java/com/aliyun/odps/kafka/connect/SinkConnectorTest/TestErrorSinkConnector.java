package com.aliyun.odps.kafka.connect.SinkConnectorTest;


import org.apache.kafka.connect.connector.Task;


public class TestErrorSinkConnector extends TestMCSinkConnector {

  @Override
  public Class<? extends Task> taskClass() {
    return TestErrorSinkTask.class;
  }
}
