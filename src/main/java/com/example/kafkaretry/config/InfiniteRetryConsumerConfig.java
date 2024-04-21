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
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import com.example.kafkaretry.domain.SampleMessage;
import com.example.kafkaretry.exception.ExistInDenyListException;
import com.fasterxml.jackson.core.type.TypeReference;

@Configuration
public class InfiniteRetryConsumerConfig {

	public static final String TOPIC = "infinite-retry-topic";
	public static final String CONTAINER_FACTORY_BEAN_NAME = "infiniteRetryKafkaListenerContainerFactory";
	public static final String DENY_CONTAINER_FACTORY_BEAN_NAME = "denyInfiniteRetryKafkaListenerContainerFactory";

	private static final int MAX_RETRIES = Integer.MAX_VALUE;
	private static final long INITIAL_INTERVAL_MILLIS = 1_000L; // 초기 재시도 1초
	private static final long MAX_INTERVAL_MILLIS = 1_000L * 60; // 최대 1분
	private static final double MULTIPLIER = 2.0; // 다음 재시도마다 2배 씩 간격 증가

	private final String brokerAddresses;

	public InfiniteRetryConsumerConfig(@Value("${spring.kafka.bootstrap-servers}") String brokerAddresses) {
		this.brokerAddresses = brokerAddresses;
	}

	@Bean(CONTAINER_FACTORY_BEAN_NAME)
	KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, SampleMessage>> factory() {
		ConcurrentKafkaListenerContainerFactory<String, SampleMessage> factory =
			new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory(RetrySupports.GROUP_ID));
		factory.setConcurrency(1);
		factory.setCommonErrorHandler(
			new DefaultErrorHandler(
				getInfiniteRetryBackOff()
			)
		);

		return factory;
	}

	@Bean(DENY_CONTAINER_FACTORY_BEAN_NAME)
	KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, SampleMessage>> denyFactory() {
		ConcurrentKafkaListenerContainerFactory<String, SampleMessage> factory =
			new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory(RetrySupports.GROUP_ID + "-dltgroup"));
		factory.setConcurrency(1);

		DefaultErrorHandler handler = new DefaultErrorHandler(
			new DeadLetterPublishingRecoverer(
				new KafkaTemplate<String, SampleMessage>(
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
				)
			),
			getInfiniteRetryBackOff()
		);

		handler.setBackOffFunction((record, exception) ->
			exception instanceof ExistInDenyListException
				? new FixedBackOff(0, 0)
				: null
		);

		factory.setCommonErrorHandler(handler);

		return factory;
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

	private BackOff getInfiniteRetryBackOff() {
		ExponentialBackOffWithMaxRetries result = new ExponentialBackOffWithMaxRetries(MAX_RETRIES);

		result.setInitialInterval(INITIAL_INTERVAL_MILLIS);
		result.setMaxInterval(MAX_INTERVAL_MILLIS);
		result.setMultiplier(MULTIPLIER);

		return result;
	}
}
