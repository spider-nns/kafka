package dev.spider.kafka.consumer;

import dev.spider.kafka.serializer.CompanyDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.util.Lists;
import org.assertj.core.util.Maps;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class SeekConsumer {

    private static final String brokerList = "c1:9092";
    private static final String topic = "topic-demo";
    private static final String groupId = "group.demo";

    private Properties properties;

    @BeforeAll
    public Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-demo");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        this.properties = properties;
        return properties;
    }

    @Test
    public void seek(String[] args) {
        KafkaConsumer consumer = new KafkaConsumer<String, String>(initConfig());
        Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
        consumer.subscribe(Arrays.asList(brokerList), new ConsumerRebalanceListener() {
            /**
             * 在再均衡开始之前和消费者停止读取消息之后被调用
             * 可以处理消费位移的提交避免一些不必要的重复消费发生
             * @param partitions 再均衡前所分配到的分区
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    consumer.committed(partition);
                    currentOffset.clear();
                }
            }

            /**
             * 重新分配分区之后和消费者开始读取消息之前被调用
             * @param partitions 再均衡后分配到的分区
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                //do st
            }
        });
        Set<TopicPartition> topicPartitions = Sets.newHashSet();
        while (topicPartitions.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            topicPartitions = consumer.assignment();
        }
        //获取指定分区末尾消息位置
        //request.timeout.ms 设置等待获取超时时间
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
        //获取分区起始位置
        Map<TopicPartition, Long> beginOffsets = consumer.beginningOffsets(topicPartitions);

        //直接从分区头/尾重置消费位移
        consumer.seekToBeginning(topicPartitions);
        consumer.seekToEnd(topicPartitions.stream().filter(topicPartition -> topicPartition.equals("")).
                collect(Collectors.toList()));
        for (TopicPartition tp : topicPartitions) {
            consumer.seek(tp, endOffsets.get(tp));
        }

        //根据时间点消费
        Map<TopicPartition, Long> timestampToSearch = Maps.newHashMap(Lists.newArrayList(topicPartitions).get(0), 100L);


        Map<TopicPartition, Long> tpTimeMap = new HashMap<>();
        for (TopicPartition topicPartition : topicPartitions) {
            tpTimeMap.put(topicPartition, System.currentTimeMillis() - 1 * 24 * 3600 * 1000);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimesMap = consumer.offsetsForTimes(tpTimeMap);
        for (TopicPartition topicPartition : topicPartitions) {
            OffsetAndTimestamp offsetAndTimestamp = offsetsForTimesMap.get(topicPartition);
            if (Objects.nonNull(offsetAndTimestamp)) {
                consumer.seek(topicPartition, offsetAndTimestamp.offset());
            }
        }
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    currentOffset.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                }
                consumer.commitAsync(currentOffset,null);

            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }

    }
}
