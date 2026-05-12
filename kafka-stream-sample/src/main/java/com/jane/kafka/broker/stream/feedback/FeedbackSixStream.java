package com.jane.kafka.broker.stream.feedback;

import com.jane.kafka.broker.message.FeedbackMessage;
import com.jane.kafka.broker.serde.FeedbackSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class FeedbackSixStream {

    private static final Logger LOG = LoggerFactory.getLogger(FeedbackSixStream.class);

    private static final Set<String> GOOD_WORDS = Set.of("good", "excellent", "great", "amazing", "fantastic", "positive", "satisfied", "happy", "love");
    private static final Set<String> BAD_WORDS = Set.of("bad", "sad", "angry", "terrible", "awful", "horrible", "negative", "unsatisfied", "unhappy", "hate");

    @Autowired
    void kstreamFeedback(StreamsBuilder builder) {
        var feedbackSerde = new FeedbackSerde();

        builder.stream("t-commodity-feedback", Consumed.with(Serdes.String(), feedbackSerde))
                .flatMap(extractWords())
                .split()
                .branch(isGoodWord(), Branched.withConsumer(
                        ks ->{
                            ks.repartition(Repartitioned.as("t-commodity-feedback-six-good"))
                                    .groupByKey().count().toStream().to("t-commodity-feedback-six-good-count");
                            ks.groupBy(
                                    (k,v) -> v).count().toStream()
                                    .to("t-commodity-feedback-six-good-count-word");
                        }
                ))
                .branch(isBadWord(), Branched.withConsumer(
                        ks ->{
                            ks.repartition(Repartitioned.as("t-commodity-feedback-six-bad"))
                                    .groupByKey().count().toStream().to("t-commodity-feedback-six-bad-count");
                            ks.groupBy(
                                            (k,v) -> v).count().toStream()
                                    .to("t-commodity-feedback-six-bad-count-word");
                        }
                ));
    }

    private Predicate<String, String> isGoodWord() {
        return (k, v) -> GOOD_WORDS.contains(v);
    }
    private Predicate<String, String> isBadWord() {
        return (k, v) -> BAD_WORDS.contains(v);
    }
    private KeyValueMapper<String, FeedbackMessage, Iterable<KeyValue<String, String>>> extractWords() {
        return (k, v) ->
            Arrays.asList(v.getFeedback().replaceAll("[^a-zA-Z]", " ").toLowerCase()
                    .toLowerCase().split("\\s+"))
                    .stream()
                    .distinct().map(word -> KeyValue.pair(v.getLocation(), word))
                    .collect(Collectors.toList());
    }

}
