package com.jane.kafka.broker.serde;

import com.jane.kafka.broker.message.OrderMessage;

public class OrderSerde extends CustomJsonSerde<OrderMessage> {
    public OrderSerde() {
        super(
                new CustomJsonSerializer<>(),
                new CustomJsonDeserializer<>(OrderMessage.class)
        );    }
}
