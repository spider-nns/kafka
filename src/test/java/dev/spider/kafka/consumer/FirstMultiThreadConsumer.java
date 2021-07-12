package dev.spider.kafka.consumer;

import dev.spider.kafka.serializer.CompanyDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.util.Lists;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class FirstMultiThreadConsumer {
    public static final String brokerList = "c2:9092,c1:9092,c3:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";



    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-demo");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        int consumerThreadNum = 4;
        for (int i = 0; i < consumerThreadNum; i++) {
            new KafkaConsumerThread(properties, topic).start();
        }
        //设置和分区大小一致的线程
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer(properties);
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
        int partition = partitionInfos.get(0).partition();
        for (int i = 0; i < partition - 1; i++) {
            new KafkaConsumerThread(properties, topic).start();
        }
    }

    /**
     * 每个线程顺序消费一个主题分区
     * 每个线程需要维护一个
     */
    public static class KafkaConsumerThread extends Thread {

        private KafkaConsumer<String, String> kafkaConsumer;

        public KafkaConsumerThread(Properties properties, String topic) {
            this.kafkaConsumer = new KafkaConsumer<>(properties);
            this.kafkaConsumer.subscribe(Lists.newArrayList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        //do st
                        //如果这部分处理耗时，整体消费能力下降
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                //do st
            } finally {
                kafkaConsumer.close();
            }
        }
    }
}
