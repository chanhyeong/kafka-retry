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

import com.example.kafkaretry.config.NonBlockingRetryConsumerConfig;
import com.example.kafkaretry.domain.SampleMessage;
import com.example.kafkaretry.test.EmbeddedKafkaHolder;

@SpringBootTest
class NonBlockingRetryListenerTest {

	@Autowired
	private DeadLetterHolder deadLetterHolder;

	@Autowired
	private NonBlockingRetryListener sut;

	static {
		EmbeddedKafkaHolder.getEmbeddedKafka()
			.addTopics(
				NonBlockingRetryConsumerConfig.TOPIC,
				NonBlockingRetryConsumerConfig.TOPIC + "-retry-0",
				NonBlockingRetryConsumerConfig.TOPIC + "-retry-1",
				NonBlockingRetryConsumerConfig.TOPIC + "-dlt"
			);
	}

	@Test
	void checkNonBlockingRetryPublishedResult() {
		// given
		String id = UUID.randomUUID().toString();
		var message = new SampleMessage(id, "userId");

		// when
		publish(message);

		// then - first retry
		while (deadLetterHolder.getNonBlockingFirstRetryMessageHolder(message.id()) == null) {
			// await
		}
		var firstRetryMessage = deadLetterHolder.getNonBlockingFirstRetryMessageHolder(message.id());
		then(firstRetryMessage).isNotNull()
			.satisfies((Consumer<FailedMessage>)it -> {
				then(it.message()).isEqualTo(message);
			});

		// then - second retry
		while (deadLetterHolder.getNonBlockingSecondRetryMessageHolder(message.id()) == null) {
			// await
		}
		var secondRetryMessage = deadLetterHolder.getNonBlockingSecondRetryMessageHolder(message.id());
		then(secondRetryMessage).isNotNull()
			.satisfies((Consumer<FailedMessage>)it -> {
				then(it.message()).isEqualTo(message);
			});

		// then - dlt
		while (deadLetterHolder.getNonBlockingFailedMessageHolder(message.id()) == null) {
			// await
		}
		var dltMessage = deadLetterHolder.getNonBlockingFailedMessageHolder(message.id());
		then(dltMessage).isNotNull()
			.satisfies((Consumer<FailedMessage>)it -> {
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
					NonBlockingRetryConsumerConfig.TOPIC,
					message.id(),
					message
				)
			);
		}
	}
}
