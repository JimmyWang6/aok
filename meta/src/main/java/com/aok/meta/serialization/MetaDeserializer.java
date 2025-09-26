package com.aok.meta.serialization;

import com.aok.meta.Meta;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.Map;

public class MetaDeserializer implements Deserializer<Meta> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public Meta deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) return null;
        try {
            return objectMapper.readValue(data, Meta.class);
        } catch (Exception e) {
            throw new RuntimeException("Meta deserialization failed", e);
        }
    }

    @Override
    public void close() {}
}

