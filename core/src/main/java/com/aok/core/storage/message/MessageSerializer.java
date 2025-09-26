package com.aok.core.storage.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class MessageSerializer implements Serializer<Message> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String s, Message message) {
        try {
            if (message == null){
                return null;
            }
            return objectMapper.writeValueAsBytes(message);
        } catch (Exception e) {
            throw new SerializationException("Error when serializing message", e);
        }
    }
}
