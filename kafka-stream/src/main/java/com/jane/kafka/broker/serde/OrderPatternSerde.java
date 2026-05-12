package com.jane.kafka.broker.serde;

import com.jane.kafka.broker.message.OrderMessage;
import com.jane.kafka.broker.message.OrderPatternMessage;

public class OrderPatternSerde extends CustomJsonSerde<OrderPatternMessage> {
    public OrderPatternSerde() {
        super(
                new CustomJsonSerializer<>(),
                new CustomJsonDeserializer<>(OrderPatternMessage.class)
        );    }
}
