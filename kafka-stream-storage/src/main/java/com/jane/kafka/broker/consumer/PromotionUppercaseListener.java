package com.jane.kafka.broker.consumer;

import com.jane.kafka.broker.message.PromotionMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PromotionUppercaseListener {
    private static final Logger LOG = LoggerFactory.getLogger(PromotionUppercaseListener.class);

    @KafkaListener(topics = "t-commodity-promotion-uppercase")
    public void listenPromotion(PromotionMessage message) {
        LOG.info("Received Promotion Message: {}", message);
    }
}
