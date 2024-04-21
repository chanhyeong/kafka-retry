package com.example.kafkaretry.config;

import java.nio.ByteBuffer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.listener.RetryListener;

/**
 * 예제 어플리케이션을 위한 공통 설정들
 */
public class RetrySupports {

	public static final String GROUP_ID = "sample-group-id";

	/**
	 * 실패 재시도마다 카운트를 ConsumerRecord 헤더에 주입 (테스트 용도)
	 */
	public static class AppendingRetryCountListener implements RetryListener {
		public static final String DELIVERY_ATTEMPT_COUNT_HEADER_NAME = "deliveryAttemptCount";

		@Override
		public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
			Iterable<Header> iterableRetryCountHeader = record.headers().headers(DELIVERY_ATTEMPT_COUNT_HEADER_NAME);
			if (iterableRetryCountHeader.iterator().hasNext()) {
				record.headers().remove(DELIVERY_ATTEMPT_COUNT_HEADER_NAME);
			}

			record.headers()
				.add(
					DELIVERY_ATTEMPT_COUNT_HEADER_NAME,
					ByteBuffer.allocate(4).putInt(deliveryAttempt).array()
				);
		}
	}
}
