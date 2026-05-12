package com.jane.kafka.broker.serde;

import com.jane.kafka.broker.message.FeedbackMessage;

public class FeedbackSerde extends CustomJsonSerde<FeedbackMessage> {
    public FeedbackSerde() {
        super(
                new CustomJsonSerializer<>(),
                new CustomJsonDeserializer<>(FeedbackMessage.class)
        );
    }
}
