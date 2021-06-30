package dev.spider.kafka.serializer;

import dev.spider.kafka.message.Company;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

/**
 * 自定义序列化
 */
public class CompanySerializer implements Serializer<Company> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Company data) {
        if (Objects.isNull(data)) {
            return null;
        }
        byte[] name, address;
        try {
            if (Objects.nonNull(data.getName())) {
                name = data.getName().getBytes("UTF-8");
            } else {
                name = new byte[0];
            }
            if (Objects.nonNull(data.getAddress())) {
                address = data.getAddress().getBytes("UTF-8");
            } else {
                address = new byte[0];
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);
            return buffer.array();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
