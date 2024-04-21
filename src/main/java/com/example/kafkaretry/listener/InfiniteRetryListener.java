package com.example.kafkaretry.listener;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.kafkaretry.config.InfiniteRetryConsumerConfig;
import com.example.kafkaretry.domain.SampleMessage;
import com.example.kafkaretry.exception.ExistInDenyListException;
import com.example.kafkaretry.holder.DenyListHolder;

@Component
class InfiniteRetryListener {

	private final DenyListHolder denyListHolder;
	private boolean succeeded;

	InfiniteRetryListener(DenyListHolder denyListHolder) {
		this.denyListHolder = denyListHolder;
	}

	@KafkaListener(
		topics = InfiniteRetryConsumerConfig.TOPIC,
		containerFactory = InfiniteRetryConsumerConfig.CONTAINER_FACTORY_BEAN_NAME
	)
	void consume(SampleMessage message) {
		if (!succeeded) {
			throw new RuntimeException("InfiniteRetryListener failed");
		}

		succeeded = true;
	}

	@KafkaListener(
		topics = InfiniteRetryConsumerConfig.TOPIC,
		containerFactory = InfiniteRetryConsumerConfig.DENY_CONTAINER_FACTORY_BEAN_NAME
	)
	void consumeWithDenyList(SampleMessage message) {
		if (denyListHolder.exists(message.id())) {
			throw new ExistInDenyListException();
		}

		if (!succeeded) {
			throw new RuntimeException("InfiniteRetryListener failed");
		}

		succeeded = true;
	}

	boolean isSucceeded() {
		return succeeded;
	}
}
