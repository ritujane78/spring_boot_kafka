package com.jane.kafka.broker.stream.feedback.rating;

import com.jane.kafka.broker.message.FeedbackRatingTwoMessage;
import com.jane.kafka.broker.serde.FeedbackRatingTwoMessageSerde;
import com.jane.kafka.broker.serde.FeedbackSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FeedbackRatingTwoStream {
    @Autowired
    void kstreamFeedbackRating(StreamsBuilder builder){
        var feedbackRatingStoreName = "feedbackRatingStore";
        var storeSupplier = Stores.inMemoryKeyValueStore(feedbackRatingStoreName);
        var storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), new FeedbackRatingTwoMessageSerde());
        builder.addStateStore(storeBuilder);

        builder.stream("t-commodity-feedback", Consumed.with(Serdes.String(), new FeedbackSerde()))
                .processValues(
                        () -> new FeedbackRatingTwoFixedKeyProcessor(feedbackRatingStoreName)
                        , feedbackRatingStoreName
                )
                .to("t-commodity-feedback-rating-two", Produced.with(Serdes.String(), new FeedbackRatingTwoMessageSerde()));
    }
}
