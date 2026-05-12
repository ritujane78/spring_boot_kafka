package com.jane.kafka.command.service;

import com.jane.kafka.api.request.FeedbackRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.jane.kafka.api.request.FeedbackRequest;
import com.jane.kafka.command.action.FeedbackAction;

@Service
public class FeedbackService {

	@Autowired
	private FeedbackAction action;

	public void createFeedback(FeedbackRequest request) {
		action.publishToKafka(request);
	}

}
