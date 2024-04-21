package com.example.kafkaretry.listener;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Component;

import com.example.kafkaretry.config.NonBlockingRetryConsumerConfig;
import com.example.kafkaretry.domain.SampleMessage;

@Component
class NonBlockingRetryListener {

	@KafkaListener(
		topics = NonBlockingRetryConsumerConfig.TOPIC,
		containerFactory = NonBlockingRetryConsumerConfig.CONTAINER_FACTORY_BEAN_NAME
	)
	@RetryableTopic(kafkaTemplate = NonBlockingRetryConsumerConfig.RETRY_PUBLISHER_BEAN_NAME)
	void consume(SampleMessage message) {
		throw new RuntimeException("NonBlockingRetryListener failed");
	}
}
