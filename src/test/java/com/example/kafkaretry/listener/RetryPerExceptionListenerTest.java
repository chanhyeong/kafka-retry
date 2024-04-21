package com.example.kafkaretry.listener;

import static org.assertj.core.api.BDDAssertions.then;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.example.kafkaretry.config.RetryPerExceptionConsumerConfig;
import com.example.kafkaretry.domain.SampleMessage;
import com.example.kafkaretry.exception.ListenerExceptions.DelayedRetry;
import com.example.kafkaretry.exception.ListenerExceptions.ImmediatePublish;
import com.example.kafkaretry.exception.ListenerExceptions.ImmediateRetry;
import com.example.kafkaretry.exception.ListenerExceptions.NoOp;
import com.example.kafkaretry.test.EmbeddedKafkaHolder;

@SpringBootTest
class RetryPerExceptionListenerTest {

	@Autowired
	private DeadLetterHolder deadLetterHolder;

	@Autowired
	private RetryPerExceptionListener sut;

	static {
		EmbeddedKafkaHolder.getEmbeddedKafka()
			.addTopics(
				RetryPerExceptionConsumerConfig.TOPIC,
				RetryPerExceptionConsumerConfig.TOPIC + "-dlq"
			);
	}

	@Test
	void checkDelayedRetryResult() {
		// given
		int retryCount = 5;
		int totalAttemptCount = 1 + retryCount; // 1번 처리 시도 + retryCount 만큼 재시도
		CountDownLatch latch = new CountDownLatch(totalAttemptCount);
		sut.testConfigure(
			latch,
			() -> new DelayedRetry("DelayedRetry", retryCount, 2000) {
			}
		);

		String id = UUID.randomUUID().toString();
		var message = new SampleMessage(id, "userId");
		publish(message);

		// when
		awaitFinishRetry(latch);

		// then
		var failedMessage = deadLetterHolder.getRetryPerExceptionFailedMessage(message.id());
		then(failedMessage).isNotNull()
			.satisfies((Consumer<FailedMessage>)it -> {
				then(it.deliveryAttemptCount()).isEqualTo(totalAttemptCount);
				then(it.message()).isEqualTo(message);
			});
	}

	@Test
	void checkImmediateRetryResult() {
		// given
		int retryCount = 10;
		int totalAttemptCount = 1 + retryCount; // 1번 처리 시도 + retryCount 만큼 재시도
		CountDownLatch latch = new CountDownLatch(totalAttemptCount);
		sut.testConfigure(
			latch,
			() -> new ImmediateRetry("ImmediateRetry", retryCount) {
			}
		);

		String id = UUID.randomUUID().toString();
		var message = new SampleMessage(id, "userId");
		publish(message);

		// when
		awaitFinishRetry(latch);

		// then
		var failedMessage = deadLetterHolder.getRetryPerExceptionFailedMessage(message.id());
		then(failedMessage).isNotNull()
			.satisfies((Consumer<FailedMessage>)it -> {
				then(it.deliveryAttemptCount()).isEqualTo(totalAttemptCount);
				then(it.message()).isEqualTo(message);
			});
	}

	@Test
	void checkImmediatePublishResult() {
		// given
		CountDownLatch latch = new CountDownLatch(1);
		sut.testConfigure(
			latch,
			() -> new ImmediatePublish("ImmediatePublish") {
			}
		);

		String id = UUID.randomUUID().toString();
		var message = new SampleMessage(id, "userId");
		publish(message);

		// when
		awaitFinishRetry(latch);

		// then
		var failedMessage = deadLetterHolder.getRetryPerExceptionFailedMessage(message.id());
		then(failedMessage).isNotNull()
			.satisfies((Consumer<FailedMessage>)it -> {
				then(it.deliveryAttemptCount()).isEqualTo(1); // 1번 처리 시도
				then(it.message()).isEqualTo(message);
			});
	}

	@Test
	void checkNoOpResult() {
		// given
		CountDownLatch latch = new CountDownLatch(1);
		sut.testConfigure(
			latch,
			() -> new NoOp() {
			}
		);

		String id = UUID.randomUUID().toString();
		var message = new SampleMessage(id, "userId");
		publish(message);

		// when
		awaitFinishRetry(latch);

		// then
		var failedMessage = deadLetterHolder.getRetryPerExceptionFailedMessage(message.id());
		then(failedMessage).isNull();
	}

	private void publish(SampleMessage message) {
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(EmbeddedKafkaHolder.getEmbeddedKafka());
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

		try (KafkaProducer<String, SampleMessage> kafkaProducer = new KafkaProducer<>(producerProps)) {
			kafkaProducer.send(
				new ProducerRecord<>(
					RetryPerExceptionConsumerConfig.TOPIC,
					message.id(),
					message
				)
			);
		}
	}

	private void awaitFinishRetry(CountDownLatch latch) {
		while (latch.getCount() > 0) {
			try {
				latch.await();
			} catch (InterruptedException e) {
				// ignore
			}
		}
	}
}
