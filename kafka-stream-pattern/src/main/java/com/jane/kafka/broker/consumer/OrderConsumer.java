package com.jane.kafka.broker.consumer;

import com.jane.kafka.broker.message.OrderMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    private static Logger logger = LoggerFactory.getLogger(OrderConsumer.class);

    @KafkaListener(topics = "t-commodity-order")
    public void listenOrder(OrderMessage message){

        var totalAmount = message.getPrice() * message.getQuantity();
        logger.info("Received order message: orderNumber={}, itemName={}, quantity={}, price={}, totalAmount={}",
                message.getOrderNumber(), message.getItemName(), message.getQuantity(), message.getPrice(), totalAmount);
    }
}
