package dev.spider.kafka.consumer;

import dev.spider.kafka.serializer.CompanyDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

class ConsumerClient {
    private static final String brokerList = "c1:9092";
    private static final String topic = "topic-demo";
    private static final String groupId = "group.demo";
    private static final String K_SER = "key.serializer";
    private static final String V_SER = "value.serializer";

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

    /**
     * 消费者步骤
     * 1.配置消费者客户端参数创建相应的消费者实例
     * 2.订阅主题
     * 3.拉取消息消费
     * 4.提交消费位移
     * 5.关闭消费者实例
     */
    @Test
    void consumerQuickStart() {
        properties = initConfig();
        //设置消费组的名称
        properties.put("group.id", groupId);
        //创建一个消费者客户端实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //订阅主题 多个主题，集合/正则(正则可以匹配到后面添加的新主题)
        //topic/pattern/assign 三种订阅方式互斥，只能使用一个
        //subscribe 方法订阅的主题具有消费者自动在均衡的功能，实现负载均衡和自动转移
        //assign 不具备 因为 ConsumerRebalanceListener
        consumer.subscribe(Collections.singletonList(topic));
        //前后订阅不一样的主题，以最后一个为准
        consumer.subscribe(Collections.singletonList(topic));
        //assign 指定分区集合
        consumer.assign(Collections.singletonList(new TopicPartition("", 1)));
        List<TopicPartition> topicPartitionList = Lists.newArrayList();
        //获取元数据
        List<PartitionInfo> partitionInfos = consumer.partitionsFor("", Duration.ofMillis(5000));
        for (PartitionInfo partitionInfo : partitionInfos) {
            int partition = partitionInfo.partition();
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            topicPartitionList.add(topicPartition);
        }
        consumer.assign(topicPartitionList);

        /*
         取消订阅
         consumer.subscribe(Collections.emptyList());
         consumer.assign(Collections.emptyList());
         */
        consumer.unsubscribe();
        //取消订阅

        //消费主题
        try {

            while (true) {
                ConsumerRecords<String, String> consumerRecord = consumer.poll(Duration.ofMillis(1000));
                //消息集合使用迭代器
                Iterator<ConsumerRecord<String, String>> iterator = consumerRecord.iterator();
                //获取指定分区 根据  topic topicPartition
                //根据订阅主题处理
                Iterable<ConsumerRecord<String, String>> records = consumerRecord.records("");
                List<ConsumerRecord<String, String>> records1 = consumerRecord.records(new TopicPartition("", 1));
                //count 消息个数
                int count = consumerRecord.count();
                //isEmpty 消息集是否为空
                boolean empty = consumerRecord.isEmpty();
                //空消息集 empty
                ConsumerRecords<Object, Object> empty1 = ConsumerRecords.empty();

                for (ConsumerRecord<String, String> record : consumerRecord) {
                    System.out.println("topic: " + record.topic() + ",partition: " + record.partition() + ",offset: " + record.offset());
                    System.out.println("key: " + record.key() + ",value: " + record.value());
                    //do something
                    if (record.serializedKeySize()==-1){
                        //key 为空
                    }
                    if (record.serializedValueSize()==-1){
                        //value 为空
                    }
                    long checksum = record.checksum();
                }
                Set<TopicPartition> partitions = consumerRecord.partitions();
                for (TopicPartition partition : partitions) {
                    List<ConsumerRecord<String, String>> partitionRecord = consumerRecord.records(partition);
                }
            }
        } catch (Exception e) {
            //log
        } finally {
            consumer.close();
        }

    }

}
