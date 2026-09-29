package com.aliyun.odps.kafka.connect.converter;

import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.TIMESTAMP_MS;
import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.OFFSET;
import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.PARTITION_ID;
import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.TOPIC_NAME;
import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.col;
import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.sinkRecord;
import static com.aliyun.odps.type.TypeInfoFactory.STRING;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.kafka.connect.converter.RecordConverterBuilder.Mode;
import com.aliyun.odps.kafka.connect.fixtures.Fixtures;
import org.junit.Assert;
import org.junit.Test;

/**
 * TEXT 格式（DefaultRecordConverter）的纯转换回归：不需要 Kafka，也不需要云凭据。
 */
public class DefaultRecordConverterTest {

  private static Record newRecord() {
    TableSchema schema = Fixtures.schemaWithFixedColumns(
        col(RecordConverter.KEY, STRING), col(RecordConverter.VALUE, STRING));
    return new ArrayRecord(schema);
  }

  @Test
  public void fillsFixedColumnsInEveryMode() throws Exception {
    for (Mode mode : Mode.values()) {
      Record out = newRecord();
      new DefaultRecordConverter(mode).convert(sinkRecord("k", "v"), out);
      Assert.assertEquals(TOPIC_NAME, out.getString(RecordConverter.TOPIC));
      Assert.assertEquals(Long.valueOf(PARTITION_ID), out.getBigint(RecordConverter.PARTITION));
      Assert.assertEquals(Long.valueOf(OFFSET), out.getBigint(RecordConverter.OFFSET));
      Assert.assertEquals(Long.valueOf(TIMESTAMP_MS), out.getBigint(RecordConverter.INSERT_TIME));
    }
  }

  @Test
  public void valueModeWritesOnlyValue() throws Exception {
    Record out = newRecord();
    new DefaultRecordConverter(Mode.VALUE).convert(sinkRecord("the-key", "the-value"), out);
    Assert.assertEquals("the-value", out.get(RecordConverter.VALUE));
    Assert.assertNull("KEY 模式下不应写 key 列", out.get(RecordConverter.KEY));
  }

  @Test
  public void keyModeWritesOnlyKey() throws Exception {
    Record out = newRecord();
    new DefaultRecordConverter(Mode.KEY).convert(sinkRecord("the-key", "the-value"), out);
    Assert.assertEquals("the-key", out.get(RecordConverter.KEY));
    Assert.assertNull(out.get(RecordConverter.VALUE));
  }

  @Test
  public void defaultModeWritesBothKeyAndValue() throws Exception {
    Record out = newRecord();
    new DefaultRecordConverter(Mode.DEFAULT).convert(sinkRecord("the-key", "the-value"), out);
    Assert.assertEquals("the-key", out.get(RecordConverter.KEY));
    Assert.assertEquals("the-value", out.get(RecordConverter.VALUE));
  }

  @Test
  public void nullKeyOrValueLeavesColumnUntouched() throws Exception {
    Record out = newRecord();
    new DefaultRecordConverter(Mode.DEFAULT).convert(sinkRecord(null, null), out);
    Assert.assertNull(out.get(RecordConverter.KEY));
    Assert.assertNull(out.get(RecordConverter.VALUE));
  }

  @Test
  public void nonStringPayloadIsStringified() throws Exception {
    Record out = newRecord();
    new DefaultRecordConverter(Mode.VALUE).convert(sinkRecord(null, 12345), out);
    Assert.assertEquals("12345", out.get(RecordConverter.VALUE));
  }

  @Test(expected = NullPointerException.class)
  public void modeIsRequired() {
    new DefaultRecordConverter(null);
  }
}
