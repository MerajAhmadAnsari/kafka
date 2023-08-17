package com.learning.kafka.customuserdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class UserSerializer implements Serializer<User> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String s, User user) {
        try {
            if (null != user) {
                return objectMapper.writeValueAsBytes(user);
            }

        } catch (Exception e) {
            throw new SerializationException("Error when serializing User to byte[]");
        }

        return null;
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
