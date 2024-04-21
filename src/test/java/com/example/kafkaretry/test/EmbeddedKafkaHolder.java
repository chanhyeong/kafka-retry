package com.example.kafkaretry.test;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;

public final class EmbeddedKafkaHolder {

	private static final EmbeddedKafkaBroker embeddedKafka = new EmbeddedKafkaZKBroker(1, false)
		.brokerListProperty("spring.kafka.bootstrap-servers");

	private static boolean started;

	public static EmbeddedKafkaBroker getEmbeddedKafka() {
		if (!started) {
			try {
				embeddedKafka.afterPropertiesSet();
			}
			catch (Exception e) {
				throw new KafkaException("Embedded broker failed to start", e);
			}
			started = true;
		}
		return embeddedKafka;
	}

	private EmbeddedKafkaHolder() {
		super();
	}
}
