package com.aliyun.odps.kafka.connect;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

import static com.aliyun.odps.kafka.connect.ConfigParameter.*;

class ErrorReporter implements ErrantRecordReporter {
    private final String topic;
    private final KafkaProducer producer;

    public ErrorReporter(ConnectorConfig config) {
        topic = RUNTIME_ERROR_TOPIC_NAME.getString(config);
        producer = new KafkaProducer<String, String>(producerProperties(config));
    }

    /**
     * 单独抽出来，是为了让"上报通道到底允许阻塞多久"这件事能在不连 broker 的情况下被断言：
     * 这个值以前是写死的 30 秒，而它落在 {@code put()} 的同步路径上——错误 topic 不存在时
     * 每条坏记录都要付满它。写死意味着运维只能接受"一批脏数据把 task 拖死"，所以改成可配。
     * 默认值与原来逐字相同，配置了也不改变任何数据语义。
     */
    static Properties producerProperties(ConnectorConfig config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, RUNTIME_ERROR_TOPIC_BOOTSTRAP_SERVERS.getString(config));
        //Kafka消息的序列化方式,这里先默认 String
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer");
        //请求的最长等待时间：可配，默认沿用以前写死的 30 秒
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, RUNTIME_ERROR_TOPIC_MAX_BLOCK_MS.getLong(config));
        return props;
    }

    @Override
    public Future<Void> report(SinkRecord sinkRecord, Throwable throwable) {
        Long timestamp = sinkRecord.timestamp();
        if (timestamp == RecordBatch.NO_TIMESTAMP) {
            timestamp = null;
        }
        String key = (String)sinkRecord.key();
        String value = (String)sinkRecord.value();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, timestamp,
            key, value);

        return this.producer.send(record);
    }

}
