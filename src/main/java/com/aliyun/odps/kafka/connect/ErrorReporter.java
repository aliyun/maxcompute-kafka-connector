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
