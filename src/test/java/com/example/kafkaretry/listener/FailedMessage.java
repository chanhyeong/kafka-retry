package com.example.kafkaretry.listener;

import com.example.kafkaretry.domain.SampleMessage;

public record FailedMessage(
	int deliveryAttemptCount,

	SampleMessage message
) {
}
