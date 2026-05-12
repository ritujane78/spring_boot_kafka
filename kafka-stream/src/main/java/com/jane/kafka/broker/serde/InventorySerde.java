package com.jane.kafka.broker.serde;

import com.jane.kafka.broker.message.InventoryMessage;
import com.jane.kafka.broker.message.OrderMessage;

public class InventorySerde extends CustomJsonSerde<InventoryMessage> {
    public InventorySerde() {
        super(
                new CustomJsonSerializer<>(),
                new CustomJsonDeserializer<>(InventoryMessage.class)
        );    }
}
