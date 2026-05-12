package com.jane.kafka.command.action;

import java.time.OffsetDateTime;
import java.util.concurrent.ThreadLocalRandom;

import com.jane.kafka.producer.FeedbackProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.jane.kafka.api.request.FeedbackRequest;
import com.jane.kafka.broker.message.FeedbackMessage;

@Component
public class FeedbackAction {

	@Autowired
	private FeedbackProducer producer;

	public void publishToKafka(FeedbackRequest request) {
		var message = new FeedbackMessage();
		message.setFeedback(request.getFeedback());
		message.setLocation(request.getLocation());
		message.setRating(request.getRating());
		// random date time between last 7 days - now
		message.setFeedbackDateTime(OffsetDateTime.now().minusHours(ThreadLocalRandom.current().nextLong(7 * 7)));

		producer.publish(message);
	}

}
