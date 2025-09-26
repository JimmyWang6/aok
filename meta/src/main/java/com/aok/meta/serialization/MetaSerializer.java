package com.aok.meta.serialization;

import com.aok.meta.Meta;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

public class MetaSerializer implements Serializer<Meta> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, Meta data) {
        if (data == null) return null;
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Meta serialization failed", e);
        }
    }

    @Override
    public void close() {}
}

