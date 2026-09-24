package com.aliyun.odps.kafka.connect.converter;

import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.col;
import static com.aliyun.odps.kafka.connect.fixtures.Fixtures.schema;
import static com.aliyun.odps.type.TypeInfoFactory.STRING;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.kafka.connect.converter.RecordConverterBuilder.Format;
import com.aliyun.odps.kafka.connect.converter.RecordConverterBuilder.Mode;
import org.junit.Assert;
import org.junit.Test;

/**
 * format × mode 组合到 converter 的分派契约：配错的组合必须报错，不能悄悄退化成别的格式。
 */
public class RecordConverterBuilderTest {

  private static final TableSchema SCHEMA = schema(col("id", STRING));

  @Test
  public void defaultsToTextValueConverter() {
    RecordConverter converter = new RecordConverterBuilder().build();
    Assert.assertTrue(converter instanceof DefaultRecordConverter);
  }

  @Test
  public void textFormatBuildsDefaultConverter() {
    for (Mode mode : Mode.values()) {
      RecordConverter converter = new RecordConverterBuilder().format(Format.TEXT).mode(mode).build();
      Assert.assertTrue(mode.name(), converter instanceof DefaultRecordConverter);
    }
  }

  @Test
  public void binaryAndJsonFormats() {
    Assert.assertTrue(new RecordConverterBuilder().format(Format.BINARY).mode(Mode.VALUE).build()
                      instanceof BinaryRecordConverter);
    Assert.assertTrue(new RecordConverterBuilder().format(Format.JSON).mode(Mode.VALUE).build()
                      instanceof JsonRecordConverter);
  }

  @Test
  public void csvFormatRequiresSchema() {
    RecordConverterBuilder builder = new RecordConverterBuilder()
        .format(Format.CSV).mode(Mode.VALUE).csvDelimiter(";");
    try {
      builder.build();
      Assert.fail("CSV 缺少 schema 时应报错");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("schema is null"));
    }
    Assert.assertTrue(builder.schema(SCHEMA).build() instanceof CsvRecordConverter);
  }

  @Test
  public void csvDefaultModeIsRejected() {
    try {
      new RecordConverterBuilder().format(Format.CSV).mode(Mode.DEFAULT).schema(SCHEMA).build();
      Assert.fail("CSV + DEFAULT 没有定义列语义，应报错");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("Unsupported combination"));
    }
  }

  @Test
  public void flattenFormatBuildsFlattenConverter() {
    TableSchema flattenSchema = schema(col(RecordConverter.TOPIC, STRING),
                                       col(RecordConverter.PARTITION,
                                           com.aliyun.odps.type.TypeInfoFactory.BIGINT),
                                       col(RecordConverter.OFFSET,
                                           com.aliyun.odps.type.TypeInfoFactory.BIGINT),
                                       col(RecordConverter.INSERT_TIME,
                                           com.aliyun.odps.type.TypeInfoFactory.BIGINT),
                                       col("id", STRING));
    Assert.assertTrue(new RecordConverterBuilder()
                      .format(Format.FLATTEN).mode(Mode.VALUE).schema(flattenSchema).build()
                      instanceof FlattenRecordConverter);
  }
}
