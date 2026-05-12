package com.jane.kafka.command.action;

import com.jane.kafka.api.request.OrderRequest;
import com.jane.kafka.broker.message.OrderMessage;
import com.jane.kafka.entity.Order;
import com.jane.kafka.entity.OrderItem;
import com.jane.kafka.broker.producer.OrderProducer;
import com.jane.kafka.repository.OrderItemRepository;
import com.jane.kafka.repository.OrderRepository;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;

@Component
public class OrderAction {

    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final OrderProducer orderProducer;

    public OrderAction(OrderRepository orderRepository, OrderItemRepository orderItemRepository, OrderProducer orderProducer) {
        this.orderRepository = orderRepository;
        this.orderItemRepository = orderItemRepository;
        this.orderProducer = orderProducer;
    }
    public Order convertToOrder(OrderRequest orderRequest) {
        System.out.println("convertToOrder: " + orderRequest);
        var order = new Order();
        order.setOrderLocation(orderRequest.getOrderLocation());
        order.setCreditCardNumber(orderRequest.getCreditCardNumber());
        order.setOrderItems(orderRequest.getOrderItems().stream().map(orderItemRequest -> {
            var orderItem = new OrderItem();
            orderItem.setProductName(orderItemRequest.getItemName());
            orderItem.setQuantity(orderItemRequest.getQuantity());
            orderItem.setPrice(orderItemRequest.getPrice());
            orderItem.setOrder(order);

            return orderItem;
        }).toList());
        order.setOrderDate(OffsetDateTime.now());
        order.setOrderNumber(RandomStringUtils.randomAlphanumeric(8).toUpperCase());
        return order;
    }

    public void saveToDatabase(Order orderEntity) {
        orderRepository.save(orderEntity);
        orderEntity.getOrderItems().forEach(orderItemRepository::save);
    }

    public OrderMessage convertToOrderMessage(OrderItem orderItem) {
        var orderMessage = new OrderMessage();
        orderMessage.setItemName(orderItem.getProductName());
        orderMessage.setQuantity(orderItem.getQuantity());
        orderMessage.setPrice(orderItem.getPrice());
        orderMessage.setOrderNumber(orderItem.getOrder().getOrderNumber());
        orderMessage.setOrderDate(orderItem.getOrder().getOrderDate());
        orderMessage.setCreditCardNumber(orderItem.getOrder().getCreditCardNumber());
        orderMessage.setOrderLocation(orderItem.getOrder().getOrderLocation());

        return orderMessage;
    }

    public void sendToKafka(OrderMessage orderMessage) {
        orderProducer.sendOrder(orderMessage);
    }
}
