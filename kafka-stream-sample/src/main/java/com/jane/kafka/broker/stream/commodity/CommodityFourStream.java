package com.jane.kafka.broker.stream.commodity;

import com.jane.kafka.broker.message.OrderPatternMessage;
import com.jane.kafka.broker.message.OrderRewardMessage;
import com.jane.kafka.broker.serde.OrderPatternSerde;
import com.jane.kafka.broker.serde.OrderRewardSerde;
import com.jane.kafka.broker.serde.OrderSerde;
import com.jane.kafka.util.CommodityStreamUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.jane.kafka.broker.message.OrderMessage;
import com.jane.kafka.broker.message.OrderPatternMessage;
import com.jane.kafka.broker.message.OrderRewardMessage;
import com.jane.kafka.util.CommodityStreamUtil;
import org.springframework.stereotype.Component;

//@Component
public class CommodityFourStream {
        private static final Logger LOG = LoggerFactory.getLogger(CommodityFourStream.class);

        private void reportFraud(OrderMessage orderMessage){
                LOG.info("Fraud detected, more details: {}", orderMessage);
        }

//        @Autowired
        void kstreamCommodityTrading(StreamsBuilder builder) {
                var orderSerde = new OrderSerde();

                var maskedCreditCardStream = builder
                                .stream("t-commodity-order", Consumed.with(Serdes.String(), orderSerde))
                                .mapValues(CommodityStreamUtil::maskCreditCardNumber);

                maskedCreditCardStream.filter(
                        (k, v)-> v.getOrderLocation().toUpperCase().startsWith("C")
                ).foreach((k, v)-> reportFraud(v));
        }

}
