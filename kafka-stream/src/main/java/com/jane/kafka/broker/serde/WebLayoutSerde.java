package com.jane.kafka.broker.serde;

import com.jane.kafka.broker.message.InventoryMessage;
import com.jane.kafka.broker.message.WebLayoutVoteMessage;

public class WebLayoutSerde extends CustomJsonSerde<WebLayoutVoteMessage> {
    public WebLayoutSerde() {
        super(
                new CustomJsonSerializer<>(),
                new CustomJsonDeserializer<>(WebLayoutVoteMessage.class)
        );    }
}
