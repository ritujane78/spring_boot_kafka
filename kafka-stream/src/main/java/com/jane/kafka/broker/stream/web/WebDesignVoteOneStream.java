package com.jane.kafka.broker.stream.web;

import com.jane.kafka.broker.message.WebDesignVoteMessage;
import com.jane.kafka.broker.serde.WebColorSerde;
import com.jane.kafka.broker.serde.WebDesignSerde;
import com.jane.kafka.broker.serde.WebLayoutSerde;
import com.jane.kafka.util.WebColorVoteTimestampExtractor;
import com.jane.kafka.util.WebLayoutVoteTimestampExtractor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WebDesignVoteOneStream {
    private WebDesignVoteMessage voteJoiner(String color, String layout){
        var result = new WebDesignVoteMessage();
        result.setColor(color);
        result.setLayout(layout);

        return result;
    }

    @Autowired
    public void kstreamWebDesignVote(StreamsBuilder builder){
        var stringSerde = Serdes.String();
        var webColorSerde = new WebColorSerde();
        var webLayoutSerde = new WebLayoutSerde();
        var webDesignSerde =  new WebDesignSerde();

        var colorTable = builder.stream("t-commodity-web-vote-color",
                        Consumed.with(stringSerde,webColorSerde)
                                .withTimestampExtractor(new WebColorVoteTimestampExtractor()))
                        .mapValues(
                                (values) -> values.getColor()
                        )
                .toTable(
                        Materialized.with(stringSerde, stringSerde)
                );
        var layoutTable = builder.stream("t-commodity-web-vote-layout",
                Consumed.with(stringSerde,webLayoutSerde)
                        .withTimestampExtractor(new WebLayoutVoteTimestampExtractor()))
                .mapValues(
                        (values) -> values.getLayout()
                )
                .toTable(
                        Materialized.with(stringSerde, stringSerde)
                );
        var joinTable = colorTable.join(
                layoutTable,
                (color, layout) -> voteJoiner(color, layout),
                Materialized.with(stringSerde, webDesignSerde)
        );
        joinTable.toStream().to("t-commodity-web-one-vote-result");

        joinTable.groupBy(
                (username, userDesign) -> KeyValue.pair(userDesign.getColor(), userDesign.getColor()),
                        Grouped.with(stringSerde, stringSerde)

                ).count()
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel("color-count"));

        joinTable.groupBy(
                        (username, userDesign) -> KeyValue.pair(userDesign.getLayout(), userDesign.getLayout()),
                        Grouped.with(stringSerde, stringSerde)

                ).count()
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel("layout-count"));

    }
}
