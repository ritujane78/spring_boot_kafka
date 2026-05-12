package com.jane.kafka.broker.serde;

import com.jane.kafka.broker.message.FeedbackMessage;
import com.jane.kafka.broker.stream.feedback.rating.FeedbackRatingTwoStoreValue;

public class FeedbackRatingTwoStoreValueSerde extends CustomJsonSerde<FeedbackRatingTwoStoreValue> {
    public FeedbackRatingTwoStoreValueSerde() {
        super(
                new CustomJsonSerializer<>(),
                new CustomJsonDeserializer<>(FeedbackRatingTwoStoreValue.class)
        );
    }
}
