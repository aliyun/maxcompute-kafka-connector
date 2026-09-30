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

  /** 显式的 JSON null 字段：这一列写成 NULL，而不是把整条记录变成一条 NullPointerException。 */
  @Test
  public void explicitJsonNullFieldValueWritesNullColumn() throws Exception {
    Record out = newRecord();
    converter().convert(sinkRecord(null, "{\"id\":null,\"name\":\"x\"}"), out);
    Assert.assertNull("显式 null 不是坏记录", out.get("id"));
    Assert.assertEquals("x", out.getString("name"));
  }

  /** 字段值为 null 且嵌套对象为 null：同样只是这一列 NULL。 */
  @Test
  public void nullNestedObjectWritesNullColumn() throws Exception {
    Record out = newRecord();
    converter().convert(sinkRecord(null, "{\"id\":1,\"extend\":null}"), out);
    Assert.assertNull(out.get("extend"));
    Assert.assertEquals(Long.valueOf(1L), out.getBigint("id"));
  }

  /** 类型变化：BIGINT 列拿到文本 "abc" 必须报错（不能静默 NULL）。 */
  @Test
  public void nonNumericTextForBigintColumnFails() {
    try {
      converter().convert(sinkRecord(null, "{\"id\":\"abc\"}"), newRecord());
      Assert.fail("类型不匹配必须报错");
    } catch (Exception e) {
      Assert.assertTrue(String.valueOf(e), e.getMessage().contains("abc"));
    }
  }

  /** 坏编码：替换字符原样透传。 */
  @Test
  public void replacementCharactersPassThroughUnchanged() throws Exception {
    Record out = newRecord();
    converter().convert(sinkRecord(null, "{\"name\":\"a\uFFFDb\"}"), out);
    Assert.assertEquals("a\uFFFDb", out.getString("name"));
  }

  /** 超长值：转换阶段不拦截，交给落盘那一步（处置见 BufferedWriterRecordErrorPolicyTest）。 */
  @Test
  public void oversizedStringValueIsAcceptedByTheConverter() throws Exception {
    StringBuilder big = new StringBuilder();
    for (int i = 0; i < 500000; i++) {
      big.append('x');
    }
    Record out = newRecord();
    converter().convert(sinkRecord(null, "{\"name\":\"" + big + "\"}"), out);
    Assert.assertEquals(500000, out.getString("name").length());
  }
}
