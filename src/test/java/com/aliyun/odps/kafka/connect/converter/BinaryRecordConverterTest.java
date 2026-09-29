package com.aliyun.odps.kafka.connect.converter;

import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.col;
import static com.aliyun.odps.type.TypeInfoFactory.BINARY;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.kafka.connect.converter.RecordConverterBuilder.Mode;
import com.aliyun.odps.kafka.connect.fixtures.Fixtures;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

/**
 * BINARY 格式（BinaryRecordConverter）的纯转换回归。
 */
public class BinaryRecordConverterTest {

  private static final byte[] KEY_BYTES = new byte[] {1, 2, 3};
  private static final byte[] VALUE_BYTES = new byte[] {4, 5, 6, 7};

  private static ArrayRecord newRecord() {
    TableSchema schema = Fixtures.schemaWithFixedColumns(
        col(RecordConverter.KEY, BINARY), col(RecordConverter.VALUE, BINARY));
    return new ArrayRecord(schema);
  }

  private static SinkRecord byteRecord() {
    return Fixtures.sinkRecord(KEY_BYTES, VALUE_BYTES);
  }

  @Test
  public void valueModeWritesValueBytes() throws Exception {
    ArrayRecord out = newRecord();
    new BinaryRecordConverter(Mode.VALUE).convert(byteRecord(), out);
    Assert.assertArrayEquals(VALUE_BYTES, out.getBinary(RecordConverter.VALUE).data());
  }

  @Test
  public void keyModeWritesKeyBytes() throws Exception {
    ArrayRecord out = newRecord();
    new BinaryRecordConverter(Mode.KEY).convert(byteRecord(), out);
    Assert.assertArrayEquals(KEY_BYTES, out.getBinary(RecordConverter.KEY).data());
  }

  @Test
  public void defaultModeWritesBoth() throws Exception {
    ArrayRecord out = newRecord();
    new BinaryRecordConverter(Mode.DEFAULT).convert(byteRecord(), out);
    Assert.assertArrayEquals(KEY_BYTES, out.getBinary(RecordConverter.KEY).data());
    Assert.assertArrayEquals(VALUE_BYTES, out.getBinary(RecordConverter.VALUE).data());
  }

  @Test
  public void bytesAreStoredAsBinaryNotString() throws Exception {
    ArrayRecord out = newRecord();
    new BinaryRecordConverter(Mode.VALUE).convert(byteRecord(), out);
    Object raw = out.get(RecordConverter.VALUE);
    Assert.assertTrue("BINARY 列应写入 Binary，实际 " + raw.getClass(), raw instanceof Binary);
  }
}
