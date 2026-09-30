package com.aliyun.odps.kafka.connect.converter;

import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.col;
import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.sinkRecord;
import static com.aliyun.odps.type.TypeInfoFactory.BINARY;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Record;
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

  /**
   * 脏数据矩阵（本项验收 A1）：BINARY 模式的类型变化——载荷不是 byte[] 时抛 ClassCastException，
   * 走错误分支（不会静默写成空二进制）。
   */
  @Test
  public void stringPayloadInBinaryModeFails() {
    TableSchema schema = Fixtures.schemaWithFixedColumns(col("value", BINARY));
    try {
      new BinaryRecordConverter(Mode.VALUE).convert(sinkRecord(null, "not-bytes"), new ArrayRecord(schema));
      Assert.fail("BINARY 模式拿到非 byte[] 必须报错");
    } catch (ClassCastException expected) {
      Assert.assertTrue(String.valueOf(expected), expected.getMessage().contains("cannot be cast"));
    }
  }

  /**
   * BINARY 模式的墓碑（value 为 NULL）在转换阶段不报错：它写入的是包着 null 的 Binary，
   * 最终落成 NULL 还是序列化失败由 tunnel 侧决定，本项未改这条路径，也没有真实服务端证据，
   * 因此这里只固定"转换阶段不报错"这一事实，不下结论。
   */
  @Test
  public void nullValueInBinaryModeIsNotRejectedAtConvertTime() {
    TableSchema schema = Fixtures.schemaWithFixedColumns(col("value", BINARY));
    Record out = new ArrayRecord(schema);
    new BinaryRecordConverter(Mode.VALUE).convert(sinkRecord(null, null), out);
    Assert.assertNotNull("Binary 模式下 null 载荷仍会调用 set（未改动路径，仅固定现状）",
                         out.get("value"));
  }
}
