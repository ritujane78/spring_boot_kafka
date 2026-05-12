package com.jane.kafka.broker.serde;

import com.jane.kafka.broker.message.PromotionMessage;

public class PromotionSerde extends CustomJsonSerde<PromotionMessage> {

    public PromotionSerde() {
        super(
                new CustomJsonSerializer<>(),
                new CustomJsonDeserializer<>(PromotionMessage.class)
        );
    }
}