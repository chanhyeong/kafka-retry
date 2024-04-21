package com.example.kafkaretry.config;

import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
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
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import com.example.kafkaretry.config.RetrySupports.AppendingRetryCountListener;
import com.example.kafkaretry.domain.SampleMessage;
import com.example.kafkaretry.exception.ListenerExceptions;
import com.fasterxml.jackson.core.type.TypeReference;

@Configuration
public class RetryPerExceptionConsumerConfig {
	public static final String TOPIC = "retry-per-exception-topic";
	public static final String CONTAINER_FACTORY_BEAN_NAME = "retryPerExceptionKafkaListenerContainerFactory";

	private final String brokerAddresses;

	public RetryPerExceptionConsumerConfig(@Value("${spring.kafka.bootstrap-servers}") String brokerAddresses) {
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
		return new RetryPerExceptionErrorHandler(brokerAddresses);
	}

	private static class RetryPerExceptionErrorHandler extends DefaultErrorHandler {
		private static final BackOff DEFAULT_BACKOFF = new FixedBackOff(0, 1); // 간격 없이 1번만 재시도

		RetryPerExceptionErrorHandler(String brokerAddresses) {
			super(
				new DeadLetterPublishingRecoverer(
					new KafkaTemplate<String, SampleMessage>(
						new DefaultKafkaProducerFactory<>(
							Map.of(
								ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddresses,
								ProducerConfig.ACKS_CONFIG, "1",
								ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1"
							),
							new StringSerializer(),
							new JsonSerializer<>(
								new TypeReference<>() {
								}
							)
						)
					),
					// 토픽마다 다른 dlq 설정 및 파티션 할당 정책 변경 가능
					(record, exception) ->
						new TopicPartition(record.topic() + "-dlq", record.partition())
				) {
					@Override
					public void accept(ConsumerRecord<?, ?> record, Consumer<?, ?> consumer, Exception exception) {
						Throwable actual = exception instanceof ListenerExecutionFailedException failedException
							? failedException.getCause()
							: exception;

						if (actual instanceof ListenerExceptions.NoOp) {
							return;
						}

						super.accept(record, consumer, exception);
					}
				},
				DEFAULT_BACKOFF
			);

			super.setRetryListeners(new AppendingRetryCountListener()); // 재시도 카운트 헤더 주입 추가
			super.setBackOffFunction((record, exception) ->
				exception instanceof ListenerExceptions listenerExceptions
					? listenerExceptions.getBackOff() // 개별 정의한 exception 으로 진행
					: DEFAULT_BACKOFF
			);
			super.addNotRetryableExceptions(ListenerExceptions.NoOp.class);
		}
	}
}
