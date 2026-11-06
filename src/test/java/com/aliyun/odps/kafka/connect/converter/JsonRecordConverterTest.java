package com.aliyun.odps.kafka.connect.converter;

import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.col;
import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.sinkRecord;
import static com.aliyun.odps.type.TypeInfoFactory.JSON;
import static com.aliyun.odps.type.TypeInfoFactory.STRING;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.JsonValue;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.kafka.connect.converter.RecordConverterBuilder.Mode;
import com.aliyun.odps.kafka.connect.fixtures.Fixtures;
import com.aliyun.odps.kafka.connect.utils.JsonHandler;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Test;

/**
 * JSON 格式（JsonRecordConverter）的纯转换回归：不访问服务，也不需要云凭据。
 */
public class JsonRecordConverterTest {

  private static Record newRecord() {
    TableSchema schema = Fixtures.schemaWithFixedColumns(
        col(RecordConverter.KEY, JSON), col(RecordConverter.VALUE, JSON));
    return new ArrayRecord(schema);
  }

  @Test
  public void writesJsonStringPayloadAsJsonValue() throws Exception {
    Record out = newRecord();
    new JsonRecordConverter(Mode.VALUE).convert(sinkRecord(null, "{\"a\":1,\"b\":\"x\"}"), out);
    JsonValue value = (JsonValue) out.get(RecordConverter.VALUE);
    Assert.assertTrue(value.isJsonObject());
    Assert.assertEquals(1L, value.get("a").getAsNumber().longValue());
    Assert.assertEquals("x", value.get("b").getAsString());
  }

  @Test
  public void writesMapPayloadAsJsonValue() throws Exception {
    Map<String, Object> payload = new HashMap<String, Object>();
    payload.put("a", 7);
    Record out = newRecord();
    new JsonRecordConverter(Mode.VALUE).convert(sinkRecord(null, payload), out);
    JsonValue value = (JsonValue) out.get(RecordConverter.VALUE);
    Assert.assertEquals(7L, value.get("a").getAsNumber().longValue());
  }

  @Test
  public void writesConnectStructPayloadWithoutSchemaEnvelope() throws Exception {
    Schema structSchema = SchemaBuilder.struct().name("payload")
        .field("name", Schema.STRING_SCHEMA).build();
    Struct struct = new Struct(structSchema).put("name", "abc");
    Record out = newRecord();
    new JsonRecordConverter(Mode.VALUE).convert(sinkRecord(null, struct), out);
    JsonValue value = (JsonValue) out.get(RecordConverter.VALUE);
    Assert.assertEquals("abc", value.get("name").getAsString());
    Map<String, Object> written = JsonHandler.json2Map(value.toString());
    Assert.assertEquals("只应有 payload 字段，不带 connect schema 包装",
                        Collections.singletonMap("name", "abc"), written);
  }

  @Test
  public void keyModeWritesKeyColumnOnly() throws Exception {
    Record out = newRecord();
    new JsonRecordConverter(Mode.KEY).convert(sinkRecord("{\"k\":true}", null), out);
    Assert.assertNotNull(out.get(RecordConverter.KEY));
    Assert.assertNull(out.get(RecordConverter.VALUE));
  }

  @Test
  public void fillsFixedColumns() throws Exception {
    Record out = newRecord();
    new JsonRecordConverter(Mode.VALUE).convert(sinkRecord(null, "{\"a\":1}"), out);
    Assert.assertEquals(Fixtures.TOPIC_NAME, out.getString(RecordConverter.TOPIC));
    Assert.assertEquals(Long.valueOf(Fixtures.OFFSET), out.getBigint(RecordConverter.OFFSET));
  }

  @Test
  public void rejectsDefaultValueInUnknownJsonType() {
    try {
      JsonRecordConverter.convertToJson(3.5d);
      Assert.fail("未识别的 payload 类型必须报错");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("no recognize type"));
    }
  }

  @Test
  public void modeDefaultIsNotSupportedForJson() throws Exception {
    try {
      new JsonRecordConverter(Mode.DEFAULT).convert(sinkRecord("k", "{\"a\":1}"), newRecord());
      Assert.fail("JSON 格式不支持 DEFAULT mode");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("Unsupported mode for jsonConverter"));
    }
  }

  // ---- 脏数据矩阵（本项验收 A1）：NULL / 字段缺失 / 超长 / 坏编码 ----

  /**
   * 墓碑记录（value 为 NULL）：转换器不写 value 这一列。它必须被理解为"这一列是 NULL"，
   * 而不是"保持载体原样"——载体是否干净由调用方（BufferedWriter）负责，
   * 跨记录的残留由 BufferedWriterRecordErrorPolicyTest 端到端钉住。
   */
  @Test
  public void nullValueTombstoneLeavesTheValueColumnUnset() throws Exception {
    Record out = newRecord();
    new JsonRecordConverter(Mode.VALUE).convert(sinkRecord(null, "{\"a\":1}"), out);
    Assert.assertNotNull(((JsonValue) out.get(RecordConverter.VALUE)));

    Record tombstone = newRecord();
    new JsonRecordConverter(Mode.VALUE).convert(sinkRecord(null, null), tombstone);
    Assert.assertNull("value 为 NULL 时不写这一列", tombstone.get(RecordConverter.VALUE));
    Assert.assertEquals(Fixtures.TOPIC_NAME, tombstone.getString(RecordConverter.TOPIC));
    Assert.assertEquals(Long.valueOf(Fixtures.OFFSET), tombstone.getBigint(RecordConverter.OFFSET));
  }

  /** KEY 模式的墓碑：key 为 NULL 时同理，只要求"不写这一列"。 */
  @Test
  public void nullKeyLeavesTheKeyColumnUnset() throws Exception {
    Record out = newRecord();
    new JsonRecordConverter(Mode.KEY).convert(sinkRecord(null, "ignored"), out);
    Assert.assertNull(out.get(RecordConverter.KEY));
  }

  /**
   * 畸形 JSON 文本（JSON 列）：在写入载体时就被 SDK 的类型校验拒掉（IllegalArgumentException），
   * 不会带着坏语法进表。异常信息只有 "Illegal argument for JsonValue value."，
   * 不含原 payload —— 定位一条坏记录要靠错误分支日志里的 topic/partition/offset。
   */
  @Test
  public void malformedJsonTextIsRejectedAtConvertTime() throws Exception {
    Record out = newRecord();
    try {
      new JsonRecordConverter(Mode.VALUE).convert(sinkRecord(null, "{not json at all"), out);
      Assert.fail("畸形 JSON 必须报错");
    } catch (IllegalArgumentException expected) {
      Assert.assertNull("载体没被写脏", out.get(RecordConverter.VALUE));
    }
  }

  /** 超长载荷（> 列大小上限量级）不在转换阶段拦截：错误只会在落盘时出现。 */
  @Test
  public void oversizedPayloadIsAcceptedByTheConverter() throws Exception {
    StringBuilder big = new StringBuilder("{\"a\":\"");
    for (int i = 0; i < 200000; i++) {
      big.append('x');
    }
    big.append("\"}");
    Record out = newRecord();
    new JsonRecordConverter(Mode.VALUE).convert(sinkRecord(null, big.toString()), out);
    Assert.assertTrue(out.get(RecordConverter.VALUE) instanceof JsonValue);
  }

  /** 坏编码：替换字符（U+FFFD）原样透传，转换器不做编码校验。 */
  @Test
  public void replacementCharactersPassThroughUnchanged() throws Exception {
    Record out = newRecord();
    new JsonRecordConverter(Mode.VALUE).convert(sinkRecord(null, "{\"a\":\"bad\uFFFDencoding\"}"), out);
    Assert.assertTrue(((JsonValue) out.get(RecordConverter.VALUE)).toString().contains("\uFFFD"));
  }

  /** 类型变化（表列类型与载荷不匹配）：JSON 载荷写进 STRING 列会在转换阶段就被拒，不会静默转成文本。 */
  @Test
  public void jsonPayloadIntoAStringColumnFails() throws Exception {
    TableSchema stringColumnSchema = Fixtures.schemaWithFixedColumns(
        col(RecordConverter.KEY, com.aliyun.odps.type.TypeInfoFactory.STRING),
        col(RecordConverter.VALUE, com.aliyun.odps.type.TypeInfoFactory.STRING));
    try {
      new JsonRecordConverter(Mode.VALUE).convert(sinkRecord(null, "{\"a\":1}"),
          new ArrayRecord(stringColumnSchema));
      Assert.fail("JSON 载荷写进 STRING 列必须报错");
    } catch (RuntimeException expected) {
      Assert.assertTrue(String.valueOf(expected), expected.getMessage().contains("STRING"));
    }
  }
}
