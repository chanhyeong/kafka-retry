package com.example.kafkaretry.listener;

import static org.assertj.core.api.BDDAssertions.then;

import java.util.Map;
import java.util.UUID;
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

import com.example.kafkaretry.config.InfiniteRetryConsumerConfig;
import com.example.kafkaretry.domain.SampleMessage;
import com.example.kafkaretry.holder.DenyListHolder;
import com.example.kafkaretry.test.EmbeddedKafkaHolder;

@SpringBootTest
class InfiniteRetryListenerTest {

	@Autowired
	private DenyListHolder denyListHolder;

	@Autowired
	private DeadLetterHolder deadLetterHolder;

	@Autowired
	private InfiniteRetryListener sut;

	static {
		EmbeddedKafkaHolder.getEmbeddedKafka()
			.addTopics(
				InfiniteRetryConsumerConfig.TOPIC,
				InfiniteRetryConsumerConfig.TOPIC + ".DLT"
			);
	}

	@Test
	void checkInfiniteRetry() {
		// given
		String id = UUID.randomUUID().toString();
		var message = new SampleMessage(id, "userId");

		// when
		publish(message);

		// then
		while (!sut.isSucceeded()) {
			// infinite loop
			// retry interval increasing until MAX_INTERVAL_MILLIS
		}
	}

	@Test
	void checkPublishedIfIdExistsInDenyList() {
		// given
		String id = UUID.randomUUID().toString();
		var message = new SampleMessage(id, "userId2");
		denyListHolder.add(id);

		// when
		publish(message);

		// then
		var failedMessage = deadLetterHolder.getInfiniteWithDenyListFailedMessage(message.id());
		while (failedMessage == null) {
			failedMessage = deadLetterHolder.getInfiniteWithDenyListFailedMessage(message.id());
		}

		then(failedMessage).isNotNull()
			.satisfies((Consumer<FailedMessage>)it -> {
				then(it.deliveryAttemptCount()).isEqualTo(0); // retry 가 일어나지 않으므로, 헤더에 값이 없음
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
					InfiniteRetryConsumerConfig.TOPIC,
					message.id(),
					message
				)
			);
		}
	}
}
