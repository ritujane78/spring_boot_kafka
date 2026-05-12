package com.jane.kafka.api.server;

import com.jane.kafka.api.request.PromotionRequest;
import com.jane.kafka.command.service.PromotionService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class PromotionApi {
    private final PromotionService promotionService;

    public PromotionApi(PromotionService promotionService) {
        this.promotionService = promotionService;
    }

    @PostMapping(value = "/promotion", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> createPromotion(@RequestBody PromotionRequest request) {
        promotionService.createPromotion(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(request.getPromotionCode());
    }
}
