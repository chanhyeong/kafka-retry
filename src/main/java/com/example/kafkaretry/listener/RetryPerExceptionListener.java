package com.example.kafkaretry.listener;

import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.kafkaretry.config.RetryPerExceptionConsumerConfig;
import com.example.kafkaretry.domain.SampleMessage;

@Component
public class RetryPerExceptionListener {

	private CountDownLatch latch;
	private Supplier<? extends RuntimeException> exceptionsSupplier;

	@KafkaListener(
		topics = RetryPerExceptionConsumerConfig.TOPIC,
		containerFactory = RetryPerExceptionConsumerConfig.CONTAINER_FACTORY_BEAN_NAME
	)
	void consume(SampleMessage message) {
		latch.countDown();
		throw exceptionsSupplier.get();
	}

	void testConfigure(CountDownLatch latch, Supplier<? extends RuntimeException> exceptionsSupplier) {
		this.latch = latch;
		this.exceptionsSupplier = exceptionsSupplier;
	}
}
