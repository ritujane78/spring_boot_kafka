package com.jane.kafka.broker.stream.commodity;

import com.jane.kafka.broker.serde.OrderPatternSerde;
import com.jane.kafka.broker.serde.OrderRewardSerde;
import com.jane.kafka.broker.serde.OrderSerde;
import com.jane.kafka.util.CommodityStreamUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

//@Component
public class CommodityOneStream {

//    @Autowired
    void kstreamCommodityTrading(StreamsBuilder builder){
        var orderSerde = new OrderSerde();
        var orderPatternSerde = new OrderPatternSerde();
        var orderRewardSerde = new OrderRewardSerde();

        var maskedCreditCardStream = builder.stream("t-commodity-order", Consumed.with(Serdes.String(), orderSerde))
                .mapValues(CommodityStreamUtil::maskCreditCardNumber);

        maskedCreditCardStream.mapValues(CommodityStreamUtil::convertToOrderPatternMessage)
                .to("t-commodity-pattern-one", Produced.with(Serdes.String(), orderPatternSerde));

        maskedCreditCardStream.filter(CommodityStreamUtil.isLargeQuantity())
                .mapValues(CommodityStreamUtil::convertToOrderRewardMessage)
                .to("t-commodity-reward-one", Produced.with(Serdes.String(), orderRewardSerde));

        maskedCreditCardStream.to("t-commodity-storage-one",Produced.with(Serdes.String(), orderSerde));

    }
}
