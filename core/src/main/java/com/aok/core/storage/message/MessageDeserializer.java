package com.aok.core.storage.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;

public class MessageDeserializer implements Deserializer<Message> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Message deserialize(String topic, byte[] data) {
        try {
            if (data == null){
                return null;
            }
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), Message.class);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to message");
        }
    }
}
