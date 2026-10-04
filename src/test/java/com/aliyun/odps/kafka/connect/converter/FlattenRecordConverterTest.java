package com.aliyun.odps.kafka.connect.converter;

import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.col;
import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.sinkRecord;
import static com.aliyun.odps.type.TypeInfoFactory.DOUBLE;
import static com.aliyun.odps.type.TypeInfoFactory.BOOLEAN;
import static com.aliyun.odps.type.TypeInfoFactory.STRING;

import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.kafka.connect.converter.RecordConverterBuilder.Mode;
import com.aliyun.odps.kafka.connect.fixtures.Fixtures;
import com.aliyun.odps.kafka.connect.utils.JsonHandler;
import org.junit.Assert;
import org.junit.Test;

/**
 * FLATTEN 格式（Json 字段拍平到列）的纯转换回归。
 */
public class FlattenRecordConverterTest {

  private static final TableSchema SCHEMA = Fixtures.schemaWithFixedColumns(
      col("id", com.aliyun.odps.type.TypeInfoFactory.BIGINT),
      col("name", STRING),
      col("score", DOUBLE),
      col("is_fired", BOOLEAN),
      col("extend", STRING));

  private static Record newRecord() {
    return new ArrayRecord(SCHEMA);
  }

  private static FlattenRecordConverter converter() {
    return new FlattenRecordConverter(Mode.VALUE, SCHEMA);
  }

  @Test
  public void flattensJsonFieldsIntoColumns() throws Exception {
    Record out = newRecord();
    converter().convert(sinkRecord(null,
        "{\"ID\":20230421,\"Name\":\"shiqing\",\"Score\":3.75,\"is_fired\":true}"), out);
    Assert.assertEquals(Long.valueOf(20230421L), out.getBigint("id"));
    Assert.assertEquals("shiqing", out.getString("name"));
    Assert.assertEquals(Double.valueOf(3.75d), out.getDouble("score"));
    Assert.assertEquals(Boolean.TRUE, out.getBoolean("is_fired"));
  }

  @Test
  public void keepsFixedColumnsAndIgnoresAbsentFields() throws Exception {
    Record out = newRecord();
    converter().convert(sinkRecord(null, "{\"name\":\"only\"}"), out);
    Assert.assertEquals("only", out.getString("name"));
    Assert.assertNull("JSON 里没有的列保持未写入", out.get("id"));
    Assert.assertEquals(Fixtures.TOPIC_NAME, out.getString(RecordConverter.TOPIC));
  }

  @Test
  public void nestedObjectIsStoredAsJsonString() throws Exception {
    Record out = newRecord();
    converter().convert(sinkRecord(null,
        "{\"name\":\"x\",\"extend\":{\"name\":\"zhangsan\",\"poi\":\"reset\"}}"), out);
    Map<String, Object> extend = JsonHandler.json2Map((String) out.get("extend"));
    Assert.assertEquals("zhangsan", extend.get("name"));
    Assert.assertEquals("reset", extend.get("poi"));
  }

  @Test
  public void acceptsMapPayloadDirectly() throws Exception {
    Map<String, Object> payload = new HashMap<String, Object>();
    payload.put("name", "from-map");
    Record out = newRecord();
    converter().convert(sinkRecord(null, payload), out);
    Assert.assertEquals("from-map", out.getString("name"));
  }

  @Test
  public void unknownJsonFieldFails() throws Exception {
    try {
      converter().convert(sinkRecord(null, "{\"no_such_column\":1}"), newRecord());
      Assert.fail("目标表没有这一列时必须报错，而不是丢数据");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("no_such_column"));
    }
  }

  @Test
  public void schemaWithoutFixedColumnsFailsAtConstruction() {
    TableSchema broken = Fixtures.schema(col("id", com.aliyun.odps.type.TypeInfoFactory.BIGINT));
    try {
      new FlattenRecordConverter(Mode.VALUE, broken);
      Assert.fail("schema 缺少 topic/partition/offset/insert_time 时应立即报错");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("no column name"));
    }
  }

  @Test
  public void modeDefaultIsNotSupportedForFlatten() throws Exception {
    try {
      new FlattenRecordConverter(Mode.DEFAULT, SCHEMA)
          .convert(sinkRecord(null, "{\"name\":\"x\"}"), newRecord());
      Assert.fail("FLATTEN 不支持 DEFAULT mode");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("Unsupported mode for FlattenConverter"));
    }
  }
  @Test(expected = IllegalArgumentException.class)
  public void malformedBooleanFailsConversion() throws Exception {
    converter().convert(sinkRecord(null, "{\"is_fired\":\"tru\"}"), newRecord());
  }
}
