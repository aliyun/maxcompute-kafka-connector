package com.aliyun.odps.kafka.connect.SinkConnectorTest;

import com.aliyun.odps.kafka.connect.MaxComputeSinkTask;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class TestSinkReadTask extends TestMCSinkTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(MaxComputeSinkTask.class);

  @Override
  public void put(Collection<SinkRecord> collection) {
    for (SinkRecord r : collection) {
      // to debug some things
      ObjectMapper objectMapper = new ObjectMapper();
    }
  }
}
