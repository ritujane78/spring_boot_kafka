package com.jane.kafka.api.request;

import com.jane.kafka.entity.OrderItem;

import java.util.List;

public class OrderRequest {
    private String orderLocation;

    private String creditCardNumber;

    private List<OrderItemRequest> orderItems;

    public OrderRequest() {
    }

    public OrderRequest(String orderLocation, String creditCardNumber, List<OrderItemRequest> orderItems) {
        this.orderLocation = orderLocation;
        this.creditCardNumber = creditCardNumber;
        this.orderItems = orderItems;
    }

    public String getOrderLocation() {
        return orderLocation;
    }

    public void setOrderLocation(String orderLocation) {
        this.orderLocation = orderLocation;
    }

    public String getCreditCardNumber() {
        return creditCardNumber;
    }

    public void setCreditCardNumber(String creditCardNumber) {
        this.creditCardNumber = creditCardNumber;
    }
    public List<OrderItemRequest> getOrderItems() {
        return orderItems;
    }

    public void setOrderItems(List<OrderItemRequest> orderItems) {
        this.orderItems = orderItems;
    }

    @Override
    public String toString() {
        return "OrderRequest{" +
                "orderLocation='" + orderLocation + '\'' +
                ", creditCardNumber='" + creditCardNumber + '\'' +
                '}';
    }
}
