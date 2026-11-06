package com.aliyun.odps.kafka.connect.converter;

import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.col;
import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.sinkRecord;
import static com.aliyun.odps.type.TypeInfoFactory.BIGINT;
import static com.aliyun.odps.type.TypeInfoFactory.BOOLEAN;
import static com.aliyun.odps.type.TypeInfoFactory.DOUBLE;
import static com.aliyun.odps.type.TypeInfoFactory.STRING;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.kafka.connect.converter.RecordConverterBuilder.Mode;
import com.aliyun.odps.kafka.connect.fixtures.Fixtures;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class CsvRecordConverterTest {

    @Test
    public void parsesCommaDelimitedRecordsByDefault() throws IOException {
        Assert.assertArrayEquals(
            new String[] {"first", "second", "third"},
            CsvRecordConverter.load("first,second,third", ','));
    }

    @Test
    public void parsesTabDelimitedRecords() throws IOException {
        Assert.assertArrayEquals(
            new String[] {"first", "second", "third"},
            CsvRecordConverter.load("first\tsecond\tthird", '\t'));
    }

    @Test
    public void acceptsTabCharacterAndEscapedTabConfiguration() {
        Assert.assertEquals('\t', CsvRecordConverter.parseDelimiter("\t"));
        Assert.assertEquals('\t', CsvRecordConverter.parseDelimiter("\\t"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsMultiCharacterDelimiter() {
        CsvRecordConverter.parseDelimiter("||");
    }

    @Test
    public void convertsTypedColumnsByPosition() throws IOException {
        TableSchema schema = Fixtures.schemaWithFixedColumns(
            col("name", STRING), col("qty", BIGINT), col("price", DOUBLE));
        Record out = new ArrayRecord(schema);

        new CsvRecordConverter(schema, Mode.VALUE, ",").convert(sinkRecord(null, "widget,3,1.5"), out);

        Assert.assertEquals(Fixtures.TOPIC_NAME, out.getString(RecordConverter.TOPIC));
        Assert.assertEquals(Long.valueOf(Fixtures.OFFSET), out.getBigint(RecordConverter.OFFSET));
        Assert.assertEquals("widget", out.getString("name"));
        Assert.assertEquals(Long.valueOf(3L), out.getBigint("qty"));
        Assert.assertEquals(Double.valueOf(1.5d), out.getDouble("price"));
    }

    @Test
    public void honorsConfiguredDelimiter() throws IOException {
        TableSchema schema = Fixtures.schemaWithFixedColumns(
            col("name", STRING), col("qty", BIGINT));
        Record out = new ArrayRecord(schema);

        new CsvRecordConverter(schema, Mode.KEY, "\t").convert(sinkRecord("a\t2", null), out);

        Assert.assertEquals("a", out.getString("name"));
        Assert.assertEquals(Long.valueOf(2L), out.getBigint("qty"));
    }

    @Test
    public void columnCountMismatchFails() {
        TableSchema schema = Fixtures.schemaWithFixedColumns(col("name", STRING), col("qty", BIGINT));
        Record out = new ArrayRecord(schema);
        try {
            new CsvRecordConverter(schema, Mode.VALUE, ",").convert(sinkRecord(null, "only-one"), out);
            Assert.fail("列数不匹配必须报错，不能少写列");
        } catch (Exception e) {
            Assert.assertTrue(String.valueOf(e.getMessage()),
                              e.getMessage().contains("Column count doesn't match"));
        }
    }

    @Test
    public void modeDefaultIsNotSupportedForCsv() {
        TableSchema schema = Fixtures.schemaWithFixedColumns(col("name", STRING));
        try {
            new CsvRecordConverter(schema, Mode.DEFAULT, ",").convert(sinkRecord("k", "v"),
                                                                      new ArrayRecord(schema));
            Assert.fail("CSV + DEFAULT 没有定义取哪一侧数据，应报错");
        } catch (Exception e) {
            Assert.assertTrue(String.valueOf(e.getMessage()),
                              e.getMessage().contains("Unsupported mode for CsvConverter"));
        }
    }

    // ---- 脏数据矩阵（本项验收 A1）：NULL / 类型变化 / 超长 / 坏编码 ----

    /** \N 是 CSV 的 NULL 记号：这一列必须是 SQL NULL，而不是字符串 "\N"。 */
    @Test
    public void nullTokenColumnBecomesSqlNull() throws IOException {
        TableSchema schema = Fixtures.schemaWithFixedColumns(col("name", STRING), col("qty", BIGINT));
        Record out = new ArrayRecord(schema);
        new CsvRecordConverter(schema, Mode.VALUE, ",").convert(sinkRecord(null, "widget,\\N"), out);
        Assert.assertNull(out.get("qty"));
        Assert.assertEquals("widget", out.getString("name"));
    }

    /**
     * CSV 是按位置逐列覆盖的：同一个载体被复用时，后一条里的 \N 会把前一条留下的值清成 NULL。
     * 这条用例钉住"CSV 不受载体复用影响"这一事实，和 JSON/FLATTEN 的缺失字段行为对照。
     */
    @Test
    public void reusedCarrierIsFullyOverwrittenColumnByColumn() throws IOException {
        TableSchema schema = Fixtures.schemaWithFixedColumns(col("name", STRING), col("qty", BIGINT));
        Record reused = new ArrayRecord(schema);
        CsvRecordConverter converter = new CsvRecordConverter(schema, Mode.VALUE, ",");

        converter.convert(sinkRecord(null, "widget,3"), reused);
        Assert.assertEquals(Long.valueOf(3L), reused.getBigint("qty"));

        converter.convert(sinkRecord(null, "\\N,4"), reused);
        Assert.assertNull("上一条的 name 不得留在这一条里", reused.get("name"));
        Assert.assertEquals(Long.valueOf(4L), reused.getBigint("qty"));
    }

    /** 类型变化：BIGINT 列拿到非数字文本必须报错，不能静默写成 NULL。 */
    @Test
    public void nonNumericTextForBigintColumnFails() {
        TableSchema schema = Fixtures.schemaWithFixedColumns(col("name", STRING), col("qty", BIGINT));
        try {
            new CsvRecordConverter(schema, Mode.VALUE, ",").convert(sinkRecord(null, "widget,three"),
                                                                    new ArrayRecord(schema));
            Assert.fail("类型不匹配必须报错");
        } catch (Exception e) {
            Assert.assertTrue(String.valueOf(e), e.getMessage().contains("Parse Error while trans value"));
        }
    }

    /**
     * 特征用例（不是断言"这样最好"）：BOOLEAN 列用 {@code Boolean.valueOf}，
     * 除 "true"（忽略大小写）以外的一切文本都变成 false，且不报错。
     * 也就是说 1/0、yes/no 这类脏值会被静默解释成 false。改动它属于行为变更，不在本项范围，
     * 这里先把它固定成可重放的现状。
     */
    @Test
    public void booleanColumnCoercesAnyNonTrueTextToFalse() throws IOException {
        TableSchema schema = Fixtures.schemaWithFixedColumns(col("flag", BOOLEAN));
        Record out = new ArrayRecord(schema);
        CsvRecordConverter converter = new CsvRecordConverter(schema, Mode.VALUE, ",");

        converter.convert(sinkRecord(null, "maybe"), out);
        Assert.assertEquals(Boolean.FALSE, out.getBoolean("flag"));

        converter.convert(sinkRecord(null, "1"), out);
        Assert.assertEquals(Boolean.FALSE, out.getBoolean("flag"));

        converter.convert(sinkRecord(null, "TRUE"), out);
        Assert.assertEquals(Boolean.TRUE, out.getBoolean("flag"));
    }

    /**
     * 超长值（CSV 路径）：随包的 CSV 解析器有 100,000 字符/列的安全上限，超过就抛 IOException，
     * 属于快速失败而不是静默截断。这里固定住这个事实——它同时是一条产品限制：
     * 单列超过 100,000 字符的 CSV 记录根本进不来，报错原文也只讲列长度，不提记录里的哪一列。
     */
    @Test
    public void columnLongerThanTheParserSafetyLimitFails() {
        TableSchema schema = Fixtures.schemaWithFixedColumns(col("name", STRING), col("qty", BIGINT));
        StringBuilder big = new StringBuilder();
        for (int i = 0; i < 100001; i++) {
            big.append('x');
        }
        try {
            new CsvRecordConverter(schema, Mode.VALUE, ",").convert(sinkRecord(null, big + ",1"),
                                                                    new ArrayRecord(schema));
            Assert.fail("超过解析器上限必须报错，不能截断后写入");
        } catch (Exception e) {
            Assert.assertTrue(String.valueOf(e), e.getMessage().contains("Maximum column length"));
        }
    }

    /** 上限以内（含 8 位数级）的长列正常写入：证明上面那条不是"任何长值都失败"。 */
    @Test
    public void longButWithinSafetyLimitColumnIsAccepted() throws IOException {
        TableSchema schema = Fixtures.schemaWithFixedColumns(col("name", STRING), col("qty", BIGINT));
        StringBuilder big = new StringBuilder();
        for (int i = 0; i < 99999; i++) {
            big.append('x');
        }
        Record out = new ArrayRecord(schema);
        new CsvRecordConverter(schema, Mode.VALUE, ",").convert(sinkRecord(null, big + ",1"), out);
        Assert.assertEquals(99999, out.getString("name").length());
    }

    /** 坏编码：替换字符原样进列，不做校验也不报错。 */
    @Test
    public void replacementCharactersPassThroughUnchanged() throws IOException {
        TableSchema schema = Fixtures.schemaWithFixedColumns(col("name", STRING), col("qty", BIGINT));
        Record out = new ArrayRecord(schema);
        new CsvRecordConverter(schema, Mode.VALUE, ",").convert(sinkRecord(null, "a\uFFFDb,1"), out);
        Assert.assertEquals("a\uFFFDb", out.getString("name"));
    }

    /** 字段数比表列多（新增字段没同步表结构）：必须报错，不能截断后静默写入。 */
    @Test
    public void extraColumnFailsInsteadOfBeingTruncated() {
        TableSchema schema = Fixtures.schemaWithFixedColumns(col("name", STRING));
        try {
            new CsvRecordConverter(schema, Mode.VALUE, ",").convert(sinkRecord(null, "a,b"),
                                                                    new ArrayRecord(schema));
            Assert.fail("多出来的列不能被悄悄丢掉");
        } catch (Exception e) {
            Assert.assertTrue(String.valueOf(e), e.getMessage().contains("Column count doesn't match"));
        }
    }
}
