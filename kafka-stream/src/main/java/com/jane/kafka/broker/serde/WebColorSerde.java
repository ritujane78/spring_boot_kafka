package com.jane.kafka.broker.serde;

import com.jane.kafka.broker.message.InventoryMessage;
import com.jane.kafka.broker.message.WebColorVoteMessage;

public class WebColorSerde extends CustomJsonSerde<WebColorVoteMessage> {
    public WebColorSerde() {
        super(
                new CustomJsonSerializer<>(),
                new CustomJsonDeserializer<>(WebColorVoteMessage.class)
        );    }
}
