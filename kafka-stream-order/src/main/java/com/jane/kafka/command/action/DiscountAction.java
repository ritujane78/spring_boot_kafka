package com.jane.kafka.command.action;

import com.jane.kafka.api.request.DiscountRequest;
import com.jane.kafka.broker.message.DiscountMessage;
import com.jane.kafka.producer.DiscountProducer;
import org.springframework.stereotype.Component;

@Component
public class DiscountAction {
    private final DiscountProducer discountProducer;

    public DiscountAction(DiscountProducer discountProducer) {
        this.discountProducer = discountProducer;
    }

    public DiscountMessage convertToDiscountMessage(DiscountRequest request) {
        return(new DiscountMessage(request.getDiscountCode(), request.getDiscountPercentage()));

    }

    public void execute(DiscountMessage message) {
        discountProducer.sendDiscountMessage(message);
    }
}
