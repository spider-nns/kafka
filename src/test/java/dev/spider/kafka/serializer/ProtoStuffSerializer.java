package dev.spider.kafka.serializer;

import dev.spider.kafka.message.Company;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.util.Objects;

public class ProtoStuffSerializer {
    public byte[] serialize(String topic, Company data) {
        if (Objects.isNull(data)) {
            return null;
        }
        Schema schema = RuntimeSchema.getSchema(data.getClass());
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        byte[] protostuff;
        try {
            protostuff = ProtostuffIOUtil.toByteArray(data, schema, buffer);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            buffer.clear();
        }
        return protostuff;
    }
    public Company deserializer(String topic,byte[] data){
        if (Objects.isNull(data)){
            return null;
        }
        Schema<Company> schema = RuntimeSchema.getSchema(Company.class);
        Company company = new Company();
        ProtostuffIOUtil.mergeFrom(data,company,schema);
        return company;
    }
}
