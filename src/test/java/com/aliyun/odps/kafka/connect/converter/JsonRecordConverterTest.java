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
}
