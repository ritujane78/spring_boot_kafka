package com.jane.kafka.producer;

import com.jane.kafka.broker.message.DiscountMessage;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class DiscountProducer {

    private final KafkaTemplate<String, DiscountMessage> kafkaTemplate;

    public DiscountProducer(KafkaTemplate<String, DiscountMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendDiscountMessage(DiscountMessage discountMessage) {
        kafkaTemplate.send("t-commodity-promotion", discountMessage);
    }
}
