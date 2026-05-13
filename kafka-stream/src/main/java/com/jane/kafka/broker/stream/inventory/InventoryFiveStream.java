package com.jane.kafka.broker.stream.inventory;

import com.jane.kafka.broker.serde.InventorySerde;
import com.jane.kafka.util.InventoryTimestampExtractor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

@Component
public class InventoryFiveStream {

    @Autowired
    void kstreamInventory(StreamsBuilder builder){
        var stringSerde = Serdes.String();
        var inventorySerde = new InventorySerde();
        var longSerde = Serdes.Long();
        var inventoryTimestampExtractor = new InventoryTimestampExtractor();
        var windowLength = Duration.ofHours(1L);
        var windowSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, windowLength.toMillis());

        builder.stream("t-commodity-inventory",
                Consumed.with(stringSerde, inventorySerde)
                        .withTimestampExtractor(inventoryTimestampExtractor))
                .mapValues(
                        (k, v) ->
                                v.getType().equalsIgnoreCase("ADD")
                                        ? Long.valueOf(v.getQuantity())
                                        : -1L * v.getQuantity()
                )
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(windowLength))
                .reduce(Long::sum, Materialized.with(stringSerde, longSerde))
                .toStream()
                .peek(
                        (k, v) -> {
                            var windowStartTime = Instant.ofEpochMilli(k.window().start()).atOffset(ZoneOffset.UTC);
                            var windowEndTime = Instant.ofEpochMilli(k.window().end()).atOffset(ZoneOffset.UTC);

                            System.out.println("[" + k.key() + "@" + windowStartTime + "/" + windowEndTime + "], " + v);
                        })
                .to("t-commodity-inventory-five", Produced.with(windowSerde, longSerde));
    }
}
