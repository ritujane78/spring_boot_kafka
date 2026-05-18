package com.jane.kafka.util;

import com.jane.kafka.broker.message.InventoryMessage;
import com.jane.kafka.broker.message.WebColorVoteMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class WebColorVoteTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        if (consumerRecord.value() instanceof WebColorVoteMessage) {
            WebColorVoteMessage webColorVoteMessage = (WebColorVoteMessage) consumerRecord.value();
            return webColorVoteMessage.getVoteDateTime().toInstant().toEpochMilli();
        }
        return l;
    }
}
