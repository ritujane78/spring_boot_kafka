package com.jane.kafka.broker.serde;

import com.jane.kafka.broker.message.FeedbackMessage;
import com.jane.kafka.broker.message.FeedbackRatingTwoMessage;

public class FeedbackRatingTwoMessageSerde extends CustomJsonSerde<FeedbackRatingTwoMessage> {
    public FeedbackRatingTwoMessageSerde() {
        super(
                new CustomJsonSerializer<>(),
                new CustomJsonDeserializer<>(FeedbackRatingTwoMessage.class)
        );
    }
}
