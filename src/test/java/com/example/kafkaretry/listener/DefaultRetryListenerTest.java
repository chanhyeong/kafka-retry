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

import com.example.kafkaretry.config.DefaultRetryConsumerConfig;
import com.example.kafkaretry.domain.SampleMessage;
import com.example.kafkaretry.test.EmbeddedKafkaHolder;

@SpringBootTest
class DefaultRetryListenerTest {

	@Autowired
	private DeadLetterHolder deadLetterHolder;

	@Autowired
	private DefaultRetryListener sut;

	static {
		EmbeddedKafkaHolder.getEmbeddedKafka()
			.addTopics(
				DefaultRetryConsumerConfig.TOPIC,
				DefaultRetryConsumerConfig.TOPIC + ".DLT"
			);
	}

	@Test
	void checkDefaultRetryResult() {
		// given
		String id = UUID.randomUUID().toString();
		var message = new SampleMessage(id, "userId");
		publish(message);

		// when
		awaitFinishRetry();

		// then
		var failedMessage = deadLetterHolder.getDefaultRetryFailedMessage(message.id());
		then(failedMessage).isNotNull()
			.satisfies((Consumer<FailedMessage>)it -> {
				then(it.deliveryAttemptCount()).isEqualTo(10); // 디폴트로는 1번 처리 시도 + 9번 재시도
				then(it.message()).isEqualTo(message);
			});
	}

	private void publish(SampleMessage message) {
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(EmbeddedKafkaHolder.getEmbeddedKafka());
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

		try (KafkaProducer<String, SampleMessage> kafkaProducer = new KafkaProducer<>(producerProps)) {
			kafkaProducer.send(
				new ProducerRecord<>(
					DefaultRetryConsumerConfig.TOPIC,
					message.id(),
					message
				)
			);
		}
	}

	private void awaitFinishRetry() {
		CountDownLatch latch = sut.getLatch();

		while (latch.getCount() > 0) {
			try {
				latch.await();
			} catch (InterruptedException e) {
				// ignore
			}
		}
	}
}
