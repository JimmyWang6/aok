package com.aok.meta.serialization;

import com.aok.meta.MetaKey;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class MetaKeySerializer implements Serializer<MetaKey> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, MetaKey data) {
        if (data == null) return null;
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("MetaKey serialization failed", e);
        }
    }
}

