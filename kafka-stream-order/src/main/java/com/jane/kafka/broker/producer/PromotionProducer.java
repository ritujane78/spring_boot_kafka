package com.jane.kafka.broker.producer;

import com.jane.kafka.broker.message.PromotionMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class PromotionProducer {
    private static final Logger LOG = LoggerFactory.getLogger(PromotionProducer.class);

    private final KafkaTemplate<String, PromotionMessage> kafkaTemplate;

    public PromotionProducer(KafkaTemplate<String, PromotionMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPromotionMessage(PromotionMessage promotionMessage) {
        try {
            var sendResult = kafkaTemplate.send("t-commodity-promotion", promotionMessage)
                    .get(3, TimeUnit.SECONDS);

            LOG.info("Promotion code: {} sent successfully", sendResult.getProducerRecord().value());
        } catch (Exception e) {
            LOG.error("Failed to send promotion message: {}", e.getMessage());
        }
        LOG.info("Just a dummy message for promotion code: {}", promotionMessage.getPromotionCode());
    }
}
