package dev.spider.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.Properties;

class ProducerClient {

    private static final String brokerList = "c1:9092";
    private static final String topic = "topic-demo";

    @Test
    void produceFastStart() {
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", brokerList);
        //配置生产者客户端参数并创建 KafkaProducer 实例
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //构建所需要发送到的消息
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello,kafka");
        //发送消息
        try {
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //关闭生产者客户端实例
        producer.close();


    }
}
