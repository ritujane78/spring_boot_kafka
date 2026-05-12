package com.jane.kafka.broker.consumer;

import com.jane.kafka.broker.message.DiscountMessage;
import com.jane.kafka.broker.message.PromotionMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

//@Service
@KafkaListener(topics = "t-commodity-promotion")
public class PromotionConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PromotionConsumer.class);

    @KafkaHandler
    public void listenPromotion(PromotionMessage promotionMessage) {
        logger.info("listenPromotion: {}", promotionMessage);
    }
    @KafkaHandler
    public void listenDiscount(DiscountMessage discountMessage) {
        logger.info("listenDiscount: {}", discountMessage);
    }
}
