package com.jane.kafka.command.service;

import com.jane.kafka.api.request.DiscountRequest;
import com.jane.kafka.api.request.PromotionRequest;
import com.jane.kafka.command.action.DiscountAction;
import com.jane.kafka.command.action.PromotionAction;
import org.springframework.stereotype.Service;

@Service
public class DiscountService {
    private final DiscountAction discountAction;

    public DiscountService(DiscountAction discountAction) {
        this.discountAction = discountAction;
    }
    public void createDiscount(DiscountRequest discountRequest) {
        var message = discountAction.convertToDiscountMessage(discountRequest);
        discountAction.execute(message);
    }
}
