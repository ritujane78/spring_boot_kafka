package com.jane.kafka.broker.serde;

import org.apache.kafka.common.serialization.Serde;

public class CustomJsonSerde<T> implements Serde<T> {
    private final CustomJsonSerializer<T> serializer;
    private final CustomJsonDeserializer<T> deserializer;

    public CustomJsonSerde(CustomJsonSerializer<T> serializer, CustomJsonDeserializer<T> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public CustomJsonSerializer<T> serializer() {
        return serializer;
    }

    @Override
    public CustomJsonDeserializer<T> deserializer() {
        return deserializer;
    }
}
