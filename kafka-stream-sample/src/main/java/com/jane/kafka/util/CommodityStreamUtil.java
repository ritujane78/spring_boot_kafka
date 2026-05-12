package com.jane.kafka.util;

import com.jane.kafka.broker.message.OrderMessage;
import com.jane.kafka.broker.message.OrderPatternMessage;
import com.jane.kafka.broker.message.OrderRewardMessage;
import org.apache.kafka.streams.kstream.Predicate;

import java.time.OffsetDateTime;

public class CommodityStreamUtil {
    public static OrderMessage maskCreditCardNumber(OrderMessage orderMessage) {
        String creditCardNumber = orderMessage.getCreditCardNumber();
        String maskedCreditCardNumber = "****-****-****-" + creditCardNumber.substring(creditCardNumber.length() - 4);

        return new OrderMessage(
                orderMessage.getOrderNumber(),
                orderMessage.getOrderLocation(),
                maskedCreditCardNumber,
                orderMessage.getOrderDate(),
                orderMessage.getItemName(),
                orderMessage.getPrice(),
                orderMessage.getQuantity());
    }
    public static OrderPatternMessage convertToOrderPatternMessage(OrderMessage orderMessage) {
        String itemName = orderMessage.getItemName();
        double totalItemAmount = orderMessage.getPrice() * orderMessage.getQuantity();
        OffsetDateTime orderDateTime = orderMessage.getOrderDate();
        String orderLocation = orderMessage.getOrderLocation();
        String orderNumber = orderMessage.getOrderNumber();

        return new OrderPatternMessage(itemName, totalItemAmount, orderDateTime, orderLocation, orderNumber);
    }

    public static OrderRewardMessage convertToOrderRewardMessage(OrderMessage orderMessage) {
        String orderLocation = orderMessage.getOrderLocation();
        String orderNumber = orderMessage.getOrderNumber();
        OffsetDateTime orderDateTime = orderMessage.getOrderDate();
        String itemName = orderMessage.getItemName();
        double price = orderMessage.getPrice();
        int quantity = orderMessage.getQuantity();

        return new OrderRewardMessage(orderLocation, orderNumber, orderDateTime, itemName, price, quantity);
    }

    public static Predicate<String, OrderMessage> isLargeQuantity(){
        return (key, orderMessage) -> orderMessage.getQuantity() > 200;
    }
}
