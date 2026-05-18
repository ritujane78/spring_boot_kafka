package com.jane.kafka.command.action;

import com.jane.kafka.broker.message.WebLayoutVoteMessage;
import com.jane.kafka.broker.producer.WebLayoutVoteProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.jane.kafka.api.request.WebLayoutVoteRequest;
import com.jane.kafka.broker.message.WebLayoutVoteMessage;
import com.jane.kafka.broker.producer.WebLayoutVoteProducer;

@Component
public class WebLayoutVoteAction {

	@Autowired
	private WebLayoutVoteProducer producer;

	public void publishToKafka(WebLayoutVoteRequest request) {
		var message = new WebLayoutVoteMessage();

		message.setUsername(request.getUsername());
		message.setLayout(request.getLayout());
		message.setVoteDateTime(request.getVoteDateTime());

		producer.publish(message);
	}

}
