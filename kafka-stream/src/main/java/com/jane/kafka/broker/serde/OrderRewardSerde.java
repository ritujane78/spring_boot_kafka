package com.jane.kafka.broker.serde;

import com.jane.kafka.broker.message.OrderMessage;
import com.jane.kafka.broker.message.OrderRewardMessage;

public class OrderRewardSerde extends CustomJsonSerde<OrderRewardMessage> {

    public OrderRewardSerde() {
        super(
                new CustomJsonSerializer<>(),
                new CustomJsonDeserializer<>(OrderRewardMessage.class)
        );    }
}
