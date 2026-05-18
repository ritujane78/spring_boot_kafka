package com.jane.kafka.util;

import com.jane.kafka.broker.message.WebLayoutVoteMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class WebLayoutVoteTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        if (consumerRecord.value() instanceof WebLayoutVoteMessage) {
            WebLayoutVoteMessage webLayoutVoteMessage = (WebLayoutVoteMessage) consumerRecord.value();
            return webLayoutVoteMessage.getVoteDateTime().toInstant().toEpochMilli();
        }
        return l;
    }
}
