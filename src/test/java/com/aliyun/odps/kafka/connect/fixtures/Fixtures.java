package com.aliyun.odps.kafka.connect.fixtures;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.kafka.connect.converter.RecordConverter;
import com.aliyun.odps.type.TypeInfo;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import static com.aliyun.odps.type.TypeInfoFactory.BIGINT;
import static com.aliyun.odps.type.TypeInfoFactory.STRING;

/**
 * 纯单元测试夹具：只在内存里构造 schema 和 SinkRecord，不启动 Kafka，也不访问 MaxCompute。
 */
public final class Fixtures {

  public static final String TOPIC_NAME = "unit-test-topic";
  public static final int PARTITION_ID = 7;
  public static final long OFFSET = 42L;
  public static final long TIMESTAMP_MS = 1700000000123L;

  private Fixtures() {
  }

  public static Column col(String name, TypeInfo type) {
    return new Column(name, type);
  }

  public static TableSchema schema(Column... columns) {
    TableSchema schema = new TableSchema();
    for (Column column : columns) {
      schema.addColumn(column);
    }
    return schema;
  }

  /**
   * 连接器每条记录都会填充的固定列：topic / partition / offset / insert_time。
   */
  public static List<Column> fixedColumns() {
    return Arrays.asList(
        col(RecordConverter.TOPIC, STRING),
        col(RecordConverter.PARTITION, BIGINT),
        col(RecordConverter.OFFSET, BIGINT),
        col(RecordConverter.INSERT_TIME, BIGINT));
  }

  /**
   * 固定列 + 业务列，等价于 converter 在 tunnel 侧看到的 record schema。
   */
  public static TableSchema schemaWithFixedColumns(Column... userColumns) {
    List<Column> columns = new ArrayList<Column>(fixedColumns());
    columns.addAll(Arrays.asList(userColumns));
    return schema(columns.toArray(new Column[columns.size()]));
  }

  public static SinkRecord sinkRecord(Object key, Object value) {
    return new SinkRecord(TOPIC_NAME, PARTITION_ID, Schema.OPTIONAL_STRING_SCHEMA, key,
                          Schema.OPTIONAL_STRING_SCHEMA, value, OFFSET, TIMESTAMP_MS,
                          TimestampType.CREATE_TIME);
  }

  public static SinkRecord sinkRecord(String topic, int partition, long offset, Long timestamp,
                                      Object key, Object value) {
    return new SinkRecord(topic, partition, Schema.OPTIONAL_STRING_SCHEMA, key,
                          Schema.OPTIONAL_STRING_SCHEMA, value, offset, timestamp,
                          TimestampType.CREATE_TIME);
  }
}
