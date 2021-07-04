package dev.spider.kafka.serializer;

import dev.spider.kafka.message.Company;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

public class CompanyDeserializer implements Deserializer {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        if (Objects.isNull(data)){
            return null;
        }
        if (data.length<8){
            throw new SerializationException("Size of data received "+data+"by DemoDeserializer i shorter than expected");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen,addressLen;
        String name,address;
        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);
        try {
            name = new String(nameBytes,"UTF-8");
            address = new String(addressBytes,"UTF-8");
        }catch (Exception e){
            throw new SerializationException("Error occur when serializing");
        }
        return new Company(name,address);
    }

    @Override
    public void close() {

    }
}
