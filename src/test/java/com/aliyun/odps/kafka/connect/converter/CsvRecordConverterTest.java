package com.aliyun.odps.kafka.connect.converter;

import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.col;
import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.sinkRecord;
import static com.aliyun.odps.type.TypeInfoFactory.BIGINT;
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
}
