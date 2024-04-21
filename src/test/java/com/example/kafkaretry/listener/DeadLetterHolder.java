package com.example.kafkaretry.listener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.example.kafkaretry.TestSupports;
import com.example.kafkaretry.config.DefaultRetryConsumerConfig;
import com.example.kafkaretry.config.InfiniteRetryConsumerConfig;
import com.example.kafkaretry.config.NonBlockingRetryConsumerConfig;
import com.example.kafkaretry.config.RetryPerExceptionConsumerConfig;
import com.example.kafkaretry.config.RetrySupports.AppendingRetryCountListener;
import com.example.kafkaretry.domain.SampleMessage;

import jakarta.annotation.Nullable;

/**
 * DLQ 로 발행되었는지를 확인하기 위한, 테스트 단 리스너
 */
@Component
public class DeadLetterHolder {

	// blocking
	private final Map<String, FailedMessage> defaultRetryFailedMessageHolder = new ConcurrentHashMap<>();
	private final Map<String, FailedMessage> retryPerExceptionFailedMessageHolder = new ConcurrentHashMap<>();
	private final Map<String, FailedMessage> infiniteWithDenyListFailedMessageHolder = new ConcurrentHashMap<>();

	// non-blocking
	private final Map<String, FailedMessage> nonBlockingFirstRetryMessageHolder = new ConcurrentHashMap<>();
	private final Map<String, FailedMessage> nonBlockingSecondRetryMessageHolder = new ConcurrentHashMap<>();
	private final Map<String, FailedMessage> nonBlockingFailedMessageHolder = new ConcurrentHashMap<>();

	@KafkaListener(
		topics = DefaultRetryConsumerConfig.TOPIC + ".DLT",
		containerFactory = DefaultRetryConsumerConfig.CONTAINER_FACTORY_BEAN_NAME
	)
	void consumeDefaultRetry(
		SampleMessage message,
		@Header(AppendingRetryCountListener.DELIVERY_ATTEMPT_COUNT_HEADER_NAME) int deliveryAttemptCount
	) {
		defaultRetryFailedMessageHolder.put(message.id(), new FailedMessage(deliveryAttemptCount, message));
	}

	@KafkaListener(
		topics = RetryPerExceptionConsumerConfig.TOPIC + "-dlq",
		containerFactory = RetryPerExceptionConsumerConfig.CONTAINER_FACTORY_BEAN_NAME
	)
	void consumeRetryPerException(
		SampleMessage message,
		@Header(AppendingRetryCountListener.DELIVERY_ATTEMPT_COUNT_HEADER_NAME) int deliveryAttemptCount
	) {
		retryPerExceptionFailedMessageHolder.put(message.id(), new FailedMessage(deliveryAttemptCount, message));
	}

	@KafkaListener(
		topics = InfiniteRetryConsumerConfig.TOPIC + ".DLT",
		containerFactory = InfiniteRetryConsumerConfig.CONTAINER_FACTORY_BEAN_NAME
	)
	void consumeInfiniteWithDenyList(
		SampleMessage message,
		@Header(AppendingRetryCountListener.DELIVERY_ATTEMPT_COUNT_HEADER_NAME) @Nullable Integer deliveryAttemptCount
	) {
		infiniteWithDenyListFailedMessageHolder.put(
			message.id(),
			new FailedMessage(deliveryAttemptCount != null ? deliveryAttemptCount : 0, message)
		);
	}

	@KafkaListener(
		topics = NonBlockingRetryConsumerConfig.TOPIC + "-retry-0",
		containerFactory = NonBlockingRetryConsumerConfig.TEST_CONTAINER_FACTORY_BEAN_NAME
	)
	void consumeNonBlockingFirstRetry(SampleMessage message) {
		nonBlockingFirstRetryMessageHolder.put(
			message.id(),
			new FailedMessage(0, message)
		);
	}

	@KafkaListener(
		topics = NonBlockingRetryConsumerConfig.TOPIC + "-retry-1",
		containerFactory = NonBlockingRetryConsumerConfig.TEST_CONTAINER_FACTORY_BEAN_NAME
	)
	void consumeNonBlockingSecondRetry(SampleMessage message) {
		nonBlockingSecondRetryMessageHolder.put(
			message.id(),
			new FailedMessage(0, message)
		);
	}

	@KafkaListener(
		topics = NonBlockingRetryConsumerConfig.TOPIC + "-dlt",
		containerFactory = NonBlockingRetryConsumerConfig.TEST_CONTAINER_FACTORY_BEAN_NAME
	)
	void consumeNonBlockingDlt(SampleMessage message) {
		nonBlockingFailedMessageHolder.put(
			message.id(),
			new FailedMessage(0, message)
		);
	}

	@Nullable
	public FailedMessage getDefaultRetryFailedMessage(String id) {
		TestSupports.waitPublishing();
		return defaultRetryFailedMessageHolder.get(id);
	}

	@Nullable
	public FailedMessage getRetryPerExceptionFailedMessage(String id) {
		TestSupports.waitPublishing();
		return retryPerExceptionFailedMessageHolder.get(id);
	}

	@Nullable
	public FailedMessage getInfiniteWithDenyListFailedMessage(String id) {
		TestSupports.waitPublishing();
		return infiniteWithDenyListFailedMessageHolder.get(id);
	}

	@Nullable
	public FailedMessage getNonBlockingFirstRetryMessageHolder(String id) {
		TestSupports.waitPublishing();
		return nonBlockingFirstRetryMessageHolder.get(id);
	}

	@Nullable
	public FailedMessage getNonBlockingSecondRetryMessageHolder(String id) {
		TestSupports.waitPublishing();
		return nonBlockingSecondRetryMessageHolder.get(id);
	}

	@Nullable
	public FailedMessage getNonBlockingFailedMessageHolder(String id) {
		TestSupports.waitPublishing();
		return nonBlockingFailedMessageHolder.get(id);
	}
}
