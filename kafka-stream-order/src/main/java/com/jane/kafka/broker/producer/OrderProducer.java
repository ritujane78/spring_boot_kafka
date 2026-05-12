package com.jane.kafka.broker.producer;

import com.jane.kafka.broker.message.OrderMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;

@Service
public class OrderProducer {
    private final KafkaTemplate<String, OrderMessage> kafkaTemplate;

    private static final Logger logger = LoggerFactory.getLogger(OrderProducer.class);

    public OrderProducer(KafkaTemplate<String, OrderMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrder(OrderMessage orderMessage) {
        var producerRecord = buildProducerRecord(orderMessage);
        kafkaTemplate.send(producerRecord).whenComplete((result, ex) -> {;
            if (ex != null) {
                logger.error("Failed to send order message: " + ex.getMessage());
            } else {
                logger.info("Order message sent successfully: " + orderMessage.getOrderNumber());
            }
        });
        logger.info("just a dummy message for order {}, item: {}", orderMessage.getOrderNumber(), orderMessage.getItemName());
    }

    private ProducerRecord<String, OrderMessage> buildProducerRecord(OrderMessage orderMessage) {
        var surpriseBonus = StringUtils.startsWithIgnoreCase(orderMessage.getOrderLocation(), "A")?25:10;
        var kafkaHeaders = new ArrayList<Header>();

        var surpriseBonusHeader = new RecordHeader("surpriseBonus", Integer.toString(surpriseBonus).getBytes());

        kafkaHeaders.add(surpriseBonusHeader);

        return new ProducerRecord<>("t-commodity-order", null, orderMessage.getOrderNumber(), orderMessage, kafkaHeaders);
    }
}
