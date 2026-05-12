package com.jane.kafka.command.action;

import com.jane.kafka.api.request.PromotionRequest;
import com.jane.kafka.broker.message.PromotionMessage;
import com.jane.kafka.producer.PromotionProducer;
import org.springframework.stereotype.Component;

@Component
public class PromotionAction {
    private final PromotionProducer promotionProducer;
    public PromotionAction(PromotionProducer promotionProducer) {
        this.promotionProducer = promotionProducer;
    }

    public PromotionMessage convertToPromotionMessage(PromotionRequest request) {
        return new PromotionMessage(request.getPromotionCode());
    }
    public void sendToKafka(PromotionMessage promotionMessage) {
        promotionProducer.sendPromotionMessage(promotionMessage);
    }

}
