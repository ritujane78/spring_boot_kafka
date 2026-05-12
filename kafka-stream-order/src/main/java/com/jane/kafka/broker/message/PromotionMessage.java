package com.jane.kafka.broker.message;

import com.jane.kafka.api.request.PromotionRequest;

public class PromotionMessage {
    private String promotionCode;

    public PromotionMessage() {
    }

    public PromotionMessage(String promotionCode) {
        this.promotionCode = promotionCode;
    }

    public String getPromotionCode() {
        return promotionCode;
    }

    public void setPromotionCode(String promotionCode) {
        this.promotionCode = promotionCode;
    }

    @Override
    public String toString() {
        return "PromotionMessage{" +
                "promotionCode='" + promotionCode + '\'' +
                '}';
    }

    public static PromotionMessage from(PromotionRequest request) {
        return new PromotionMessage(request.getPromotionCode());
    }
}
