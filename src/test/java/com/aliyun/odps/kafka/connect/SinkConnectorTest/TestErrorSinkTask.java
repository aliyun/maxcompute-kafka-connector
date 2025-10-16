package com.aliyun.odps.kafka.connect.SinkConnectorTest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;

public class TestErrorSinkTask extends TestMCSinkTask {

  // 对拉取到的数据进行破坏，造成runtime Error的情景
  @Override
  public void put(Collection<SinkRecord> collection) {
    // 对partition是奇数的record，做格式的混乱
    List<SinkRecord> replaceCollection = new ArrayList<>();
    for (SinkRecord record : collection) {
      if ((record.kafkaOffset() % 2) == 1) {
        // 设置一个非法的Sink
        // String topic, int partition, Schema keySchema, Object key, Schema valueSchema, Object value, long kafkaOffset)
        replaceCollection.add(
            new SinkRecord(record.topic(), record.kafkaPartition(), record.keySchema(), null, null,
                           "error record", record.kafkaOffset()));
      } else {
        replaceCollection.add(record);
      }
    }
    super.put(replaceCollection);
  }
}
