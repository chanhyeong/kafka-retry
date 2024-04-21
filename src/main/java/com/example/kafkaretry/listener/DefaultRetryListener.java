package com.example.kafkaretry.listener;

import java.util.concurrent.CountDownLatch;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.kafkaretry.config.DefaultRetryConsumerConfig;
import com.example.kafkaretry.domain.SampleMessage;

@Component
class DefaultRetryListener {

	private final CountDownLatch latch = new CountDownLatch(10); // 첫 번째 처리 + 10번 재시도

	@KafkaListener(
		topics = DefaultRetryConsumerConfig.TOPIC,
		containerFactory = DefaultRetryConsumerConfig.CONTAINER_FACTORY_BEAN_NAME
	)
	void consume(SampleMessage message) {
		latch.countDown();
		throw new RuntimeException("DefaultRetryListener failed");
	}

	CountDownLatch getLatch() {
		return latch;
	}
}
