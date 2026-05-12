package com.jane.kafka.command.service;

import com.jane.kafka.api.request.PromotionRequest;
import com.jane.kafka.command.action.PromotionAction;
import org.springframework.stereotype.Service;

@Service
public class PromotionService {
    private final PromotionAction promotionAction;

    public PromotionService(PromotionAction promotionAction) {
        this.promotionAction = promotionAction;
    }

    public void createPromotion(PromotionRequest promotionRequest) {
        var promotionMessage = promotionAction.convertToPromotionMessage(promotionRequest);
        System.out.println(promotionMessage);
        promotionAction.sendToKafka(promotionMessage);
    }
}
