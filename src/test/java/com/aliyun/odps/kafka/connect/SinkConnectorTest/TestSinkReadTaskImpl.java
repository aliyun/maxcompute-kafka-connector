package com.aliyun.odps.kafka.connect.SinkConnectorTest;

import com.aliyun.odps.kafka.connect.SinkTaskImpl;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class TestSinkReadTaskImpl extends TestMCSinkTaskImpl {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestSinkReadTaskImpl.class);

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord r : records) {
      // to debug some things
      ObjectMapper objectMapper = new ObjectMapper();
    }
  }
}
