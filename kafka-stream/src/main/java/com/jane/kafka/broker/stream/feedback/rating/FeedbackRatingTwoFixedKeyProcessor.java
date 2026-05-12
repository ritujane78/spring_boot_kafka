package com.jane.kafka.broker.stream.feedback.rating;

import com.jane.kafka.broker.message.FeedbackMessage;
import com.jane.kafka.broker.message.FeedbackRatingTwoMessage;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class FeedbackRatingTwoFixedKeyProcessor implements FixedKeyProcessor<String, FeedbackMessage, FeedbackRatingTwoMessage> {
    private FixedKeyProcessorContext<String, FeedbackRatingTwoMessage> context;

    private final String stateStoreName;

    private KeyValueStore<String, FeedbackRatingTwoStoreValue> ratingStateStore;

    public FeedbackRatingTwoFixedKeyProcessor(String stateStoreName) {
        if(StringUtils.isEmpty(stateStoreName)) {
            throw new IllegalArgumentException("State store name must be provided");
        }
        this.stateStoreName = stateStoreName;
    }


    @Override
    public void init(FixedKeyProcessorContext<String, FeedbackRatingTwoMessage> context) {
        this.context = context;
        this.ratingStateStore = context.getStateStore(stateStoreName);
    }

    @Override
    public void process(FixedKeyRecord<String, FeedbackMessage> fixedKeyRecord) {
        var originalValue = fixedKeyRecord.value();
        var storeValue = Optional.ofNullable(ratingStateStore.get(originalValue.getLocation()))
                .orElse(new FeedbackRatingTwoStoreValue());
        var ratingMap = Optional.ofNullable(storeValue.getRatingMap())
                .orElse(new TreeMap<Integer,Long>());
        var currentRatingCount = Optional.ofNullable(ratingMap.get(originalValue.getRating()))
                .orElse(Long.valueOf(0));
        var newRatingCount = currentRatingCount + 1;

        ratingMap.put(originalValue.getRating(), newRatingCount);

        ratingStateStore.put(originalValue.getLocation(), new FeedbackRatingTwoStoreValue(ratingMap));

        var branchRating = new FeedbackRatingTwoMessage();

        branchRating.setLocation(originalValue.getLocation());
        branchRating.setRatingMap(ratingMap);
        branchRating.setAverageRating(calculateAverageRate(ratingMap));
        context.forward(fixedKeyRecord.withValue(branchRating));
    }

    private double calculateAverageRate(Map<Integer, Long> ratingMap) {
        var totalRating = ratingMap.entrySet().stream().mapToLong(entry -> (entry.getKey()) * entry.getValue()).sum();
        var totalFeedback = ratingMap.values().stream().mapToLong(Long::longValue).sum();

        return totalFeedback == 0? 0: Math.round(totalRating/totalFeedback * 10.0) / 10.0;
    }
}
