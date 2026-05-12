package com.jane.kafka.broker.stream.commodity;

import com.jane.kafka.broker.serde.OrderSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;

import com.jane.kafka.broker.message.OrderMessage;
import com.jane.kafka.util.CommodityStreamUtil;
import org.springframework.stereotype.Component;

//@Component
public class MaskOrderStream {

//    @Autowired
    void kstreamCommodityMask(StreamsBuilder builder) {
        var maskedCreditCardStream = builder.stream("t-commodity-order", Consumed.with(Serdes.String(), new OrderSerde()))
                .mapValues(CommodityStreamUtil::maskCreditCardNumber);

        maskedCreditCardStream.to("t-commodity-order-masked", Produced.with(Serdes.String(), new OrderSerde()));

        maskedCreditCardStream.print(Printed.<String, OrderMessage>toSysOut().withLabel("Masked Order Stream"));
    }

}
