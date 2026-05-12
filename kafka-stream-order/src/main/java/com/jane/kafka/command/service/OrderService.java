package com.jane.kafka.command.service;

import com.jane.kafka.api.request.OrderRequest;
import com.jane.kafka.broker.message.OrderMessage;
import com.jane.kafka.command.action.OrderAction;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final OrderAction orderAction;
    public OrderService(OrderAction orderAction) {
        this.orderAction = orderAction;
    }

    public String saveOrder(OrderRequest orderRequest) {
        var orderEntity = orderAction.convertToOrder(orderRequest);
        System.out.println(orderEntity);

        orderAction.saveToDatabase(orderEntity);

        orderEntity.getOrderItems().forEach(orderItem -> {
            OrderMessage orderMessage = orderAction.convertToOrderMessage(orderItem);
            orderAction.sendToKafka(orderMessage);
        });
        return orderEntity.getOrderNumber();
    }
}
