package dev.spider.kafka.consumer;

import dev.spider.kafka.serializer.CompanyDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThirdMultiThreadConsumer {
    public static final String brokerList = "c2:9092,c1:9092,c3:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";
    public static Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();


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
        KafkaConsumerThread kafkaConsumerThread = new KafkaConsumerThread(properties, topic, Runtime.getRuntime().availableProcessors());
        kafkaConsumerThread.start();

    }

    public static class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String, String> kafkaConsumer;
        private ExecutorService executorService;
        private Integer threadNumber;


        public KafkaConsumerThread(Properties properties, String topic, int threadNumber) {
            KafkaConsumer kafkaConsumer = new KafkaConsumer<>(properties);
            kafkaConsumer.subscribe(Collections.singleton(topic));
            this.threadNumber = threadNumber;
            this.executorService = new ThreadPoolExecutor(threadNumber, threadNumber, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = this.kafkaConsumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        executorService.submit(new RecordHandler(records));
                        synchronized (offsets) {
                            if (!offsets.isEmpty()) {
                                kafkaConsumer.commitSync(offsets);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }
        }
    }

    public static class RecordHandler extends Thread {
        public final ConsumerRecords<String, String> records;

        public RecordHandler(ConsumerRecords<String, String> records) {
            this.records = records;
        }

        @Override
        public void run() {
            //consumer records
            for (TopicPartition tp : records.partitions()) {
                List<ConsumerRecord<String, String>> tpRecords = this.records.records(tp);
                //处理tpRecords
                long lastConsumedOffset = tpRecords.get(tpRecords.size() - 1).offset();
                synchronized (offsets) {
                    if (!offsets.containsKey(tp)) {
                        offsets.put(tp, new OffsetAndMetadata(lastConsumedOffset + 1));
                    } else {
                        long position = offsets.get(tp).offset();
                        if (position < lastConsumedOffset + 1) {
                            offsets.put(tp, new OffsetAndMetadata(lastConsumedOffset + 1));
                        }
                    }
                }
            }
        }
    }
}
