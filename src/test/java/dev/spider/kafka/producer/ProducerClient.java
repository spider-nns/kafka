package dev.spider.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

class ProducerClient {

    private static final String brokerList = "c1:9092,c2:9092,c3:9092";//>2,其中宕机，仍然可以连接到集群上
    private static final String topic = "topic-demo";
    private static final String K_SER = "key.serializer";
    private static final String V_SER = "value.serializer";


    private Properties properties;

    @BeforeAll
    public Properties initConfig() {
        Properties properties = new Properties();
//        properties.put("bootstrap.service", brokerList);//指定生产者客户端连接集群所需地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);//指定生产者客户端连接集群所需地址
        //broker 端收到的消息必须以字节数组的形式存在
        //"org.apache.kafka.common.serialization.StringSerializer"
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        this.properties = properties;
        return properties;
    }

    @Test
    void produceFastStart() {
//        Properties properties = new Properties();
//        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put("bootstrap.servers", brokerList);
        //配置生产者客户端参数并创建 KafkaProducer 实例
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //构建所需要发送到的消息
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "hello,kafka");
        /**
         * public class ProducerRecord<K, V> {
         *     private final String topic; 主题
         *     private final Integer partition; 分区号
         *     private final Headers headers; 消息同步
         *     private final K key; 键 指定消息的键，可以用来计算分区号，可以对消息进行二次归类
         *     private final V value; 值
         *     private final Long timestamp; 消息的时间戳
         *}
         */
        //发送消息
        //KafkaProducer 是线程安全的
        try {
            //对于同一个分区，如果消息record1 先发送，Kafka 可以保证对应的 callback 先调用
            producer.send(record, (metadata, exception) -> {
                //metadata, exception 是互斥的
                if (exception != null) {
                    //do something log...
                    exception.printStackTrace();
                } else {
                    //
                    System.out.println(metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        //关闭生产者客户端实例
        //会阻塞等待之前所有的发送请求完成后在关闭 KafkaProduce
        producer.close(10, TimeUnit.SECONDS);//超时关闭
        producer.close();
    }
}
