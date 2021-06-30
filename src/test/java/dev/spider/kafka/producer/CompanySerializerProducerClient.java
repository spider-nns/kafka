package dev.spider.kafka.producer;

import dev.spider.kafka.interceptor.CompanyInterceptorPlus;
import dev.spider.kafka.interceptor.CompanyInterceptorPrefix;
import dev.spider.kafka.message.Company;
import dev.spider.kafka.partitioner.CompanyPartitioner;
import dev.spider.kafka.serializer.CompanySerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

class CompanySerializerProducerClient {
    private static final String brokerList = "c1:9092,c2:9092,c3:9092";
    private static final String topic = "company-serializer";

    @Test
    void send() throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        //自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CompanyPartitioner.class.getName());
        //拦截器,可以多个构成拦截链,如果某个执行失败，下一个拦截器会接着从上一个执行成功的拦截器继续执行
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CompanyInterceptorPlus.class.getName() +
                "," + CompanyInterceptorPrefix.class.getName());

        KafkaProducer<String, Company> producer = new KafkaProducer<>(properties);
        Company company = Company.builder().name("hiddenKafka").build();
        ProducerRecord<String, Company> record = new ProducerRecord<>(topic, company);
        producer.send(record).get();
    }
}
