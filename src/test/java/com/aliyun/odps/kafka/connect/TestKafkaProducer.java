package com.aliyun.odps.kafka.connect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.kafka.connect.utils.JsonHandler;

@Ignore("Need a kafka broker")
public class TestKafkaProducer {

  public static final String brokerList = "http://11.158.199.161:9092";
  public static final String topic = "perf_test2";
  private static final Logger LOGGER = LoggerFactory.getLogger(MaxComputeSinkConnector.class);

  private static final int MSG_SIZE = 1061674;
  private static final ExecutorService
      executorService =
      Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
  public static int core = 2;
  private static CountDownLatch countDownLatch = new CountDownLatch(core);

  private static class ProducerWorker implements Runnable {

    private String base;
    private KafkaProducer<String, String> producer;
    private int echo = 0;

    public ProducerWorker(int echo, String base, KafkaProducer<String, String> producer) {
      this.base = base;
      this.producer = producer;
      this.echo = echo;
    }

    @Override
    public void run() {
      //final String id = Thread.currentThread().getId() + "-" + System.identityHashCode(producer);
      try {
        for (int i = 0; i < MSG_SIZE / core; i++) {
          ProducerRecord<String, String>
              record =
              new ProducerRecord<>(topic, String.valueOf(i), base);
          producer.send(record);
          if (i % 10000 == 0) {
            System.out.println("cur:" + i + " echo " + echo);
          }
        }
//                producer.send(messageList);
//                producer.send(Arrays.asList(base));
        System.out.println(Thread.currentThread().getId() + "-" + System.identityHashCode(producer)
                           + " ok write records size: " + echo);
        countDownLatch.countDown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  @Ignore
  public void getTotalMessage() {
    Properties props = initConfig();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put("group.id", "test-group");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(topic));
    List<TopicPartition> partitions = new ArrayList<>();
    int numsPartition = 1;
    for (int i = 0; i < numsPartition; i++) {
      partitions.add(new TopicPartition(topic, i));
    }
    Map<TopicPartition, Long> offsets = consumer.endOffsets(partitions);
    Map<TopicPartition, Long> earlyOffsets = consumer.beginningOffsets(partitions);
    int totalCount = 0;
    for (TopicPartition tp : offsets.keySet()) {
      totalCount += (offsets.get(tp) - earlyOffsets.get(tp));
    }
    System.out.println("Total message count:" + totalCount);
    consumer.close();
  }

  public static Properties initConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "1048576");
    props.put(ProducerConfig.LINGER_MS_CONFIG, "50");
    props.put(ProducerConfig.ACKS_CONFIG, "0");
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    props.put("client.id", "producer.client.id.demo");
    return props;
  }

  public static void main(String[] args) {
    Properties props = initConfig();
    KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    String jsonData = JsonHandler.readJson("kafka-test.json");
    try {
      for (int i = 0; i < core; i++) {
        //ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(i), jsonData);
        executorService.submit(new ProducerWorker(i, jsonData, producer));
      }
      countDownLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      producer.close();
      executorService.shutdown();
    }
    System.out.println("write record ready !!!" + Runtime.getRuntime().availableProcessors());
  }
}
