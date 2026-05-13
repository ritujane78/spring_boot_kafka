package com.jane.kafka.util;

import com.jane.kafka.broker.message.InventoryMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class InventoryTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        if (consumerRecord.value() instanceof com.jane.kafka.broker.message.InventoryMessage) {
            InventoryMessage inventoryMessage = (InventoryMessage) consumerRecord.value();
            return inventoryMessage.getTransactionTime().toInstant().toEpochMilli();
        }
        return l;
    }
}
