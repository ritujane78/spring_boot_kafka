package com.jane.kafka.api;

import com.jane.kafka.api.request.OrderRequest;
import com.jane.kafka.api.response.OrderResponse;
import com.jane.kafka.command.service.OrderService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.awt.*;

@RestController
@RequestMapping("/api/order")
public class OrderApi {
    private OrderService orderService;

    public OrderApi(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping(value = "",produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<OrderResponse> createOrder(@RequestBody OrderRequest orderRequest) {
        var orderNumber = orderService.saveOrder(orderRequest);

        var orderResponse = new OrderResponse(orderNumber);
        System.out.println(orderNumber);
        System.out.println(orderResponse);

        return ResponseEntity.ok().body(orderResponse);
    }
}
