package com.aliyun.odps.kafka.connect;

import static com.aliyun.odps.kafka.connect.ErrorReporter.HEADER_ERROR;
import static com.aliyun.odps.kafka.connect.ErrorReporter.HEADER_ORIGIN_OFFSET;
import static com.aliyun.odps.kafka.connect.ErrorReporter.HEADER_ORIGIN_PARTITION;
import static com.aliyun.odps.kafka.connect.ErrorReporter.HEADER_ORIGIN_TOPIC;
import static com.aliyun.odps.kafka.connect.ErrorReporter.HEADER_PAYLOAD_ENCODING;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.kafka.connect.fixtures.Fixtures;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

/**
 * 自带 DLQ（runtime.error.topic.*）投递内容的回归：不需要 broker，只验证被投递的载荷与头部。
 */
public class ErrorReporterTest {

  private static final String ERROR_TOPIC = "mc-connect-errors";

  private static String header(ProducerRecord<String, String> record, String key) {
    return new String(record.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
  }

  /** 非字符串载荷不得把上报本身打爆：修复前这里是 (String) 强转，Struct/byte[] 会抛 ClassCastException。 */
  @Test
  public void structPayloadIsSerializedAsJson() {
    Schema structSchema = SchemaBuilder.struct().name("payload").field("name", Schema.STRING_SCHEMA).build();
    Struct struct = new Struct(structSchema).put("name", "abc");
    SinkRecord record = Fixtures.sinkRecord(null, struct);

    ProducerRecord<String, String> out = ErrorReporter.toErrorRecord(ERROR_TOPIC, record,
        new IllegalStateException("boom"));

    Assert.assertTrue("Struct 载荷要能被读出来，而不是抛 ClassCastException: " + out.value(),
                      out.value().contains("\"name\":\"abc\""));
  }

  @Test
  public void mapPayloadIsSerializedAsJson() {
    Map<String, Object> payload = new HashMap<String, Object>();
    payload.put("a", 7);

    Assert.assertEquals("{\"a\":7}", ErrorReporter.asText(payload));
  }

  @Test
  public void stringPayloadIsPassedThroughAndNullStaysNull() {
    Assert.assertEquals("raw", ErrorReporter.asText("raw"));
    Assert.assertNull(ErrorReporter.asText(null));
  }

  /** 二进制载荷用 base64 表达并打上编码头部，否则 [B@1a2b3c 这种地址串没有任何取证价值。 */
  @Test
  public void binaryPayloadIsBase64WithEncodingHeader() {
    byte[] bytes = new byte[] {0, 1, 2, (byte) 255};
    SinkRecord binaryRecord = new SinkRecord(Fixtures.TOPIC_NAME, Fixtures.PARTITION_ID, null, null,
                                            Schema.BYTES_SCHEMA, bytes, Fixtures.OFFSET,
                                            Long.valueOf(Fixtures.TIMESTAMP_MS), TimestampType.CREATE_TIME);

    ProducerRecord<String, String> out = ErrorReporter.toErrorRecord(ERROR_TOPIC, binaryRecord,
        new IllegalStateException("boom"));

    Assert.assertEquals(Base64.getEncoder().encodeToString(bytes), out.value());
    Assert.assertEquals("base64", header(out, HEADER_PAYLOAD_ENCODING));
  }

  /** DLQ 里的记录必须自带来源坐标和出错原因，否则事后无法判断它是从哪个 topic/partition/offset 退回来的。 */
  @Test
  public void errorRecordCarriesOriginCoordinatesAndCause() {
    SinkRecord record = Fixtures.sinkRecord("source-topic", 3, 42L, Long.valueOf(1700000000123L), null, "junk");

    ProducerRecord<String, String> out = ErrorReporter.toErrorRecord(ERROR_TOPIC, record,
        new IOExceptionLike("cannot parse record"));

    Assert.assertEquals("source-topic", header(out, HEADER_ORIGIN_TOPIC));
    Assert.assertEquals("3", header(out, HEADER_ORIGIN_PARTITION));
    Assert.assertEquals("42", header(out, HEADER_ORIGIN_OFFSET));
    Assert.assertTrue(header(out, HEADER_ERROR).contains("cannot parse record"));
    Assert.assertEquals(Long.valueOf(1700000000123L), out.timestamp());
  }

  private static final class IOExceptionLike extends RuntimeException {
    private IOExceptionLike(String message) {
      super(message);
    }
  }
}
