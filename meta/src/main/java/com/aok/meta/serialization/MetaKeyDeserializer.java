package com.aok.meta.serialization;

import com.aok.meta.MetaKey;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class MetaKeyDeserializer implements Deserializer<MetaKey> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public MetaKey deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) return null;
        try {
            return objectMapper.readValue(data, MetaKey.class);
        } catch (Exception e) {
            throw new RuntimeException("MetaKey deserialization failed", e);
        }
    }
}


