package com.aliyun.odps.kafka.connect;

import static com.aliyun.odps.kafka.connect.ConfigParameter.MAXCOMPUTE_ENDPOINT;
import static com.aliyun.odps.kafka.connect.ConfigParameter.MAXCOMPUTE_PROJECT;
import static com.aliyun.odps.kafka.connect.ConfigParameter.MAXCOMPUTE_TABLE;
import static com.aliyun.odps.kafka.connect.ConfigParameter.RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS;
import static com.aliyun.odps.kafka.connect.ConfigParameter.RUNTIME_ERROR_TOPIC_MAX_BLOCK_MS;
import static com.aliyun.odps.kafka.connect.ConfigParameter.RUNTIME_ERROR_TOPIC_NAME;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 * 上报通道 producer 的元数据等待口径回归：不连 broker、不读云凭据，属于 PR 门禁的 unit 档。
 *
 * <p>这条等待发生在 {@code put()} 的同步路径上（每条坏记录一次），以前是写死的 30 秒：
 * 死信 topic 不存在时，一批脏数据能把 task 拖到被消费组踢出，而使用者没有任何旋钮。
 * 这里钉两件事——默认值必须与写死的那个值逐字相同（默认行为不变），以及配置真的落到 producer 上。
 */
public class ErrorReporterBackoffConfigTest {

  private static ConnectorConfig config(String maxBlockMs) {
    Map<String, String> props = new HashMap<String, String>();
    props.put(MAXCOMPUTE_ENDPOINT.getName(), "http://service.example.com/api");
    props.put(MAXCOMPUTE_PROJECT.getName(), "project_placeholder");
    props.put(MAXCOMPUTE_TABLE.getName(), "table_placeholder");
    props.put(ConfigParameter.ACCESS_ID.getName(), "placeholder-id");
    props.put(ConfigParameter.ACCESS_KEY.getName(), "placeholder-key");
    props.put(RUNTIME_ERROR_TOPIC_NAME.getName(), "dlq");
    props.put(RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS.getName(), "localhost:9092");
    if (maxBlockMs != null) {
      props.put(RUNTIME_ERROR_TOPIC_MAX_BLOCK_MS.getName(), maxBlockMs);
    }
    return new ConnectorConfig(props);
  }

  private static long blockMs(Properties props) {
    return ((Number) props.get(ProducerConfig.MAX_BLOCK_MS_CONFIG)).longValue();
  }

  @Test
  public void defaultBlockWaitIsThePreviouslyHardCodedThirtySeconds() {
    Properties props = ErrorReporter.producerProperties(config(null));
    Assert.assertEquals("默认必须沿用修复前写死的 30 秒，否则这次改动就悄悄改变了现有部署的行为",
        30_000L, blockMs(props));
    Assert.assertEquals("localhost:9092", props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    Assert.assertEquals("org.apache.kafka.common.serialization.StringSerializer",
        props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
  }

  @Test
  public void configuredBlockWaitReachesTheProducer() {
    Assert.assertEquals(2_000L, blockMs(ErrorReporter.producerProperties(config("2000"))));
    // 0 是合法取值：元数据拿不到就立刻失败，坏记录马上按既有策略处理，不在 put() 里等
    Assert.assertEquals(0L, blockMs(ErrorReporter.producerProperties(config("0"))));
  }

  @Test
  public void theKnobIsDocumentedWithTheUnitItAccepts() {
    Assert.assertEquals("runtime.error.topic.max.block.ms", RUNTIME_ERROR_TOPIC_MAX_BLOCK_MS.getName());
    Assert.assertEquals(org.apache.kafka.common.config.ConfigDef.Type.LONG,
        RUNTIME_ERROR_TOPIC_MAX_BLOCK_MS.getType());
    ConnectorConfig config = config(null);
    Assert.assertEquals(30_000L, RUNTIME_ERROR_TOPIC_MAX_BLOCK_MS.getLong(config));
  }
}
