package com.jane.kafka.api.server;

import com.jane.kafka.api.request.DiscountRequest;
import com.jane.kafka.command.service.DiscountService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class DiscountApi {
    private final DiscountService discountService;

    public DiscountApi(DiscountService discountService) {
        this.discountService = discountService;
    }

    @PostMapping(value = "/discount", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> createPromotion(@RequestBody DiscountRequest request) {
        discountService.createDiscount(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(request.getDiscountCode() + "with " + request.getDiscountPercentage() +"% discount.");
    }
}
