package com.jane.kafka.broker.serde;

import com.jane.kafka.broker.message.InventoryMessage;
import com.jane.kafka.broker.message.WebDesignVoteMessage;

public class WebDesignSerde extends CustomJsonSerde<WebDesignVoteMessage> {
    public WebDesignSerde() {
        super(
                new CustomJsonSerializer<>(),
                new CustomJsonDeserializer<>(WebDesignVoteMessage.class)
        );    }
}
