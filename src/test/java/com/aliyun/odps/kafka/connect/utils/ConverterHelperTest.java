package com.aliyun.odps.kafka.connect.utils;

import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.col;
import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.schema;
import static com.aliyun.odps.type.TypeInfoFactory.BIGINT;
import static com.aliyun.odps.type.TypeInfoFactory.BOOLEAN;
import static com.aliyun.odps.type.TypeInfoFactory.DATE;
import static com.aliyun.odps.type.TypeInfoFactory.DATETIME;
import static com.aliyun.odps.type.TypeInfoFactory.DECIMAL;
import static com.aliyun.odps.type.TypeInfoFactory.DOUBLE;
import static com.aliyun.odps.type.TypeInfoFactory.JSON;
import static com.aliyun.odps.type.TypeInfoFactory.STRING;
import static com.aliyun.odps.type.TypeInfoFactory.TIMESTAMP;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.Assert;
import org.junit.Test;

/**
 * 字符串 → MaxCompute 列值的类型映射回归（CSV / FLATTEN 共用的一条路径）。
 */
public class ConverterHelperTest {

  private static ArrayRecord recordWith(TypeInfo type) {
    return new ArrayRecord(schema(new Column[] {col("c0", type)}));
  }

  @Test
  public void mapsString() throws Exception {
    ArrayRecord rec = recordWith(STRING);
    ConverterHelper.setRecordByType(rec, 0, "hello");
    Assert.assertEquals("hello", rec.getString(0));
  }

  @Test
  public void mapsBigintIncludingFullRange() throws Exception {
    ArrayRecord rec = recordWith(BIGINT);
    ConverterHelper.setRecordByType(rec, 0, String.valueOf(Long.MAX_VALUE));
    Assert.assertEquals(Long.valueOf(Long.MAX_VALUE), rec.getBigint(0));
  }

  @Test
  public void mapsDoubleSpecialTokens() throws Exception {
    ArrayRecord rec = recordWith(DOUBLE);
    ConverterHelper.setRecordByType(rec, 0, "nan");
    Assert.assertTrue(Double.isNaN(rec.getDouble(0)));
    ConverterHelper.setRecordByType(rec, 0, "inf");
    Assert.assertEquals(Double.POSITIVE_INFINITY, rec.getDouble(0), 0.0);
    ConverterHelper.setRecordByType(rec, 0, "-inf");
    Assert.assertEquals(Double.NEGATIVE_INFINITY, rec.getDouble(0), 0.0);
    ConverterHelper.setRecordByType(rec, 0, "1.25");
    Assert.assertEquals(Double.valueOf(1.25d), rec.getDouble(0));
  }

  @Test
  public void mapsBoolean() throws Exception {
    ArrayRecord rec = recordWith(BOOLEAN);
    ConverterHelper.setRecordByType(rec, 0, "true");
    Assert.assertEquals(Boolean.TRUE, rec.getBoolean(0));
  }

  @Test
  public void mapsDate() throws Exception {
    ArrayRecord rec = recordWith(DATE);
    ConverterHelper.setRecordByType(rec, 0, "2023-01-02");
    Assert.assertEquals(LocalDate.of(2023, 1, 2), rec.getDateAsLocalDate(0));
  }

  @Test
  public void mapsDatetimeWithSystemZone() throws Exception {
    ArrayRecord rec = recordWith(DATETIME);
    ConverterHelper.setRecordByType(rec, 0, "2021-11-26 00:04:00");
    ZonedDateTime expected = ZonedDateTime.of(2021, 11, 26, 0, 4, 0, 0, ZoneId.systemDefault());
    Assert.assertEquals(expected, rec.getDatetimeAsZonedDateTime(0));
  }

  @Test
  public void mapsTimestampWithMilliseconds() throws Exception {
    ArrayRecord rec = recordWith(TIMESTAMP);
    ConverterHelper.setRecordByType(rec, 0, "2017-11-11 00:00:00.1234");
    Instant expected = ZonedDateTime
        .of(2017, 11, 11, 0, 0, 0, 123400000, ZoneId.systemDefault()).toInstant();
    Assert.assertEquals(expected, rec.getTimestampAsInstant(0));
  }

  @Test
  public void mapsDecimal() throws Exception {
    ArrayRecord rec = recordWith(DECIMAL);
    ConverterHelper.setRecordByType(rec, 0, "12.500");
    Assert.assertEquals(0, new BigDecimal("12.500").compareTo(rec.getDecimal(0)));
  }

  @Test
  public void mapsJson() throws Exception {
    ArrayRecord rec = recordWith(JSON);
    ConverterHelper.setRecordByType(rec, 0, "{\"a\":1}");
    Assert.assertEquals(1L, rec.getJsonValue(0).get("a").getAsNumber().longValue());
  }

  @Test
  public void nullValueClearsColumn() throws Exception {
    ArrayRecord rec = recordWith(STRING);
    ConverterHelper.setRecordByType(rec, 0, "x");
    ConverterHelper.setRecordByType(rec, 0, null);
    Assert.assertTrue(rec.isNull(0));
    Assert.assertNull(rec.get(0));
  }

  @Test
  public void rejectsUnsupportedColumnType() {
    ArrayRecord rec = recordWith(TypeInfoFactory.getArrayTypeInfo(STRING));
    try {
      ConverterHelper.setRecordByType(rec, 0, "x");
      Assert.fail("ARRAY 这类未支持的列类型必须显式报错");
    } catch (Exception e) {
      Assert.assertTrue(String.valueOf(e.getMessage()), e.getMessage().contains("Unsupported type"));
    }
  }

  @Test
  public void malformedNumberFailsInsteadOfSilentlyDropping() {
    ArrayRecord rec = recordWith(BIGINT);
    try {
      ConverterHelper.setRecordByType(rec, 0, "not-a-number");
      Assert.fail("坏数据必须报错，不能写空值继续");
    } catch (Exception e) {
      Assert.assertTrue(e.getClass().getName(), e instanceof NumberFormatException);
    }
  }
}
