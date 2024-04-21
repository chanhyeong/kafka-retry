package com.example.kafkaretry.config;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.example.kafkaretry.domain.SampleMessage;
import com.fasterxml.jackson.core.type.TypeReference;

@Configuration
public class NonBlockingRetryConsumerConfig {
	public static final String TOPIC = "non-blocking-retry-topic";
	public static final String CONTAINER_FACTORY_BEAN_NAME = "nonBlockingRetryKafkaListenerContainerFactory";
	public static final String TEST_CONTAINER_FACTORY_BEAN_NAME = "testNonBlockingRetryKafkaListenerContainerFactory";
	public static final String RETRY_PUBLISHER_BEAN_NAME = "nonBlockingRetryPublisher";

	private final String brokerAddresses;

	public NonBlockingRetryConsumerConfig(@Value("${spring.kafka.bootstrap-servers}") String brokerAddresses) {
		this.brokerAddresses = brokerAddresses;
	}

	@Bean(CONTAINER_FACTORY_BEAN_NAME)
	KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, SampleMessage>> factory() {
		ConcurrentKafkaListenerContainerFactory<String, SampleMessage> factory =
			new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory(RetrySupports.GROUP_ID));
		factory.setConcurrency(1);

		return factory;
	}

	@Bean(TEST_CONTAINER_FACTORY_BEAN_NAME)
	KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, SampleMessage>> testFactory() {
		ConcurrentKafkaListenerContainerFactory<String, SampleMessage> factory =
			new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory(RetrySupports.GROUP_ID + "-test"));
		factory.setConcurrency(1);

		return factory;
	}

	@Bean(RETRY_PUBLISHER_BEAN_NAME)
	KafkaTemplate<String, SampleMessage> kafkaTemplate() {
		return new KafkaTemplate<>(
			new DefaultKafkaProducerFactory<>(
				Map.of(
					ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddresses,
					ProducerConfig.ACKS_CONFIG, "1",
					ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1"
				),
				new StringSerializer(),
				new JsonSerializer<>(new TypeReference<>() {
				})
			)
		);
	}

	private ConsumerFactory<String, SampleMessage> consumerFactory(String groupId) {
		return new DefaultKafkaConsumerFactory<>(
			Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddresses,
				ConsumerConfig.GROUP_ID_CONFIG, groupId,
				ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class
			),
			new StringDeserializer(),
			new ErrorHandlingDeserializer<>(new JsonDeserializer<>(SampleMessage.class))
		);
	}
}
