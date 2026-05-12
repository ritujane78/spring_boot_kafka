package com.jane.kafka.api.request;

import com.jane.kafka.broker.message.PromotionMessage;

public class PromotionRequest {
    private String promotionCode;

    public PromotionRequest() {
    }
    public PromotionRequest(String promotionCode) {
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
        return "PromotionRequest{" +
                "promotionCode='" + promotionCode + '\'' +
                '}';
    }

    public PromotionMessage toMessage() {
        return new PromotionMessage(this.promotionCode);
    }
}
