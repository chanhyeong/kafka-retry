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
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.example.kafkaretry.config.RetrySupports.AppendingRetryCountListener;
import com.example.kafkaretry.domain.SampleMessage;
import com.fasterxml.jackson.core.type.TypeReference;

@Configuration
public class DefaultRetryConsumerConfig {

	public static final String TOPIC = "default-retry-topic";
	public static final String CONTAINER_FACTORY_BEAN_NAME = "defaultRetryKafkaListenerContainerFactory";

	private final String brokerAddresses;

	public DefaultRetryConsumerConfig(@Value("${spring.kafka.bootstrap-servers}") String brokerAddresses) {
		this.brokerAddresses = brokerAddresses;
	}

	@Bean(CONTAINER_FACTORY_BEAN_NAME)
	KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, SampleMessage>> factory() {
		ConcurrentKafkaListenerContainerFactory<String, SampleMessage> factory =
			new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.setConcurrency(1);
		factory.setCommonErrorHandler(errorHandler());

		return factory;
	}

	private ConsumerFactory<String, SampleMessage> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(
			Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddresses,
				ConsumerConfig.GROUP_ID_CONFIG, RetrySupports.GROUP_ID,
				ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class
			),
			new StringDeserializer(),
			new ErrorHandlingDeserializer<>(new JsonDeserializer<>(SampleMessage.class))
		);
	}

	private CommonErrorHandler errorHandler() {
		DefaultErrorHandler errorHandler = new DefaultErrorHandler(
			buildDlqPublishingRecoverer() // dlq 발행 처리 추가
			// 개별 backoff 추가 가능
		);
		errorHandler.setRetryListeners(new AppendingRetryCountListener()); // 재시도 카운트 헤더 주입 추가

		return errorHandler;
	}

	private DeadLetterPublishingRecoverer buildDlqPublishingRecoverer() {
		return new DeadLetterPublishingRecoverer(
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
		);
	}
}
