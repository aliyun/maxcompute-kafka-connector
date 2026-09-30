package com.aliyun.odps.kafka.connect;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import com.aliyun.odps.kafka.connect.utils.JsonHandler;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aliyun.odps.kafka.connect.ConfigParameter.RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS;
import static com.aliyun.odps.kafka.connect.ConfigParameter.RUNTIME_ERROR_TOPIC_NAME;

/**
 * 连接器自带的错误上报通道：把没能写进 MaxCompute 的原始记录转发到一个独立的 Kafka topic（自带 DLQ）。
 *
 * <p>注意它和 Kafka Connect 平台的 {@code errors.tolerance} / {@code errors.deadletterqueue.topic.name}
 * 不是同一条通道：平台的 DLQ 依赖 task 把异常抛出 {@code put()}、由 worker 用它自己的
 * {@code ErrantRecordReporter} 投递；而 {@code skip_error=true} 时本连接器不抛异常，平台的 DLQ 什么都收不到。
 * 两条通道只能选一条：要么配 {@code runtime.error.topic.*}，要么保持 {@code skip_error=false} 交给 worker。
 */
class ErrorReporter implements ErrantRecordReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorReporter.class);

    /** 上报载荷带上出错原因和来源坐标，否则 DLQ 里的记录看不出它为什么被退回。 */
    static final String HEADER_ERROR = "mc-connect-error";
    static final String HEADER_ORIGIN_TOPIC = "mc-connect-origin-topic";
    static final String HEADER_ORIGIN_PARTITION = "mc-connect-origin-partition";
    static final String HEADER_ORIGIN_OFFSET = "mc-connect-origin-offset";
    static final String HEADER_PAYLOAD_ENCODING = "mc-connect-payload-encoding";

    private final String topic;
    /**
     * 保持原始类型（和修复前一致）：{@code KafkaProducer<K,V>.send(record, callback)} 返回
     * {@code Future<RecordMetadata>}，而 {@code ErrantRecordReporter#report} 要求 {@code Future<Void>}。
     */
    @SuppressWarnings("rawtypes")
    private final KafkaProducer producer;

    public ErrorReporter(ConnectorConfig config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS.getString(config));
        //Kafka消息的序列化方式,这里先默认 String
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer");
        //请求的最长等待时间
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30 * 1000);
        topic = RUNTIME_ERROR_TOPIC_NAME.getString(config);
        producer = new KafkaProducer<String, String>(props);
    }

    @Override
    public Future<Void> report(SinkRecord sinkRecord, Throwable throwable) {
        final ProducerRecord<String, String> record = toErrorRecord(topic, sinkRecord, throwable);
        final SinkRecord failed = sinkRecord;
        // send() 的 Future 原来被直接丢掉：投递失败时"已上报"的记录其实哪儿也没去过，而提交水位照样前进。
        return this.producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    LOGGER.error("Failed to report record {}-{}@{} to error topic {}; the committed MaxCompute"
                            + " offset has already moved past this record",
                        failed.topic(), failed.kafkaPartition(), failed.kafkaOffset(), topic, exception);
                }
            }
        });
    }

    /** 单独抽出来：不连 broker 也能验证 DLQ 里的载荷和头部。 */
    static ProducerRecord<String, String> toErrorRecord(String topic, SinkRecord sinkRecord, Throwable throwable) {
        Long timestamp = sinkRecord.timestamp();
        if (timestamp == RecordBatch.NO_TIMESTAMP) {
            timestamp = null;
        }
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, null, timestamp,
            asText(sinkRecord.key()), asText(sinkRecord.value()));
        addStringHeader(record, HEADER_ERROR, String.valueOf(throwable));
        addStringHeader(record, HEADER_ORIGIN_TOPIC, sinkRecord.topic());
        addStringHeader(record, HEADER_ORIGIN_PARTITION, String.valueOf(sinkRecord.kafkaPartition()));
        addStringHeader(record, HEADER_ORIGIN_OFFSET, String.valueOf(sinkRecord.kafkaOffset()));
        if (sinkRecord.value() instanceof byte[]) {
            addStringHeader(record, HEADER_PAYLOAD_ENCODING, "base64");
        }
        return record;
    }

    private static void addStringHeader(ProducerRecord<String, String> record, String key, String value) {
        record.headers().add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * 被拒记录的文本化。原来这里直接 {@code (String) sinkRecord.value()}：非字符串载荷（Struct / byte[]）
     * 会在 BufferedWriter 的 catch 块里再抛 ClassCastException —— 配了 DLQ 也照样把 task 打死，
     * 而且这条记录既没落盘也没上报。
     */
    static String asText(Object payload) {
        if (payload == null) {
            return null;
        }
        if (payload instanceof String) {
            return (String)payload;
        }
        if (payload instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[])payload);
        }
        if (payload instanceof Struct || payload instanceof Map) {
            try {
                if (payload instanceof Struct) {
                    Struct struct = (Struct)payload;
                    return JsonHandler.getJsonString(JsonHandler.extractPayLoad(struct.schema(), struct, false));
                }
                return JsonHandler.getJsonString(payload);
            } catch (Exception e) {
                LOGGER.debug("Cannot serialize the rejected payload as JSON, falling back to toString()", e);
            }
        }
        return String.valueOf(payload);
    }

    /** 关闭生产者：task 停止时把还在内存里排队的错误记录发出去，否则它们连同数据一起被丢掉。 */
    void close() {
        try {
            producer.close();
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to close the runtime error topic producer cleanly", e);
        }
    }
}
