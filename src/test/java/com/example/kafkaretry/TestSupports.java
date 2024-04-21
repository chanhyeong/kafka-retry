package com.example.kafkaretry;

import java.time.Duration;

public final class TestSupports {

	private TestSupports() {
		throw new UnsupportedOperationException("utility class");
	}

	public static void waitPublishing() {
		sleep(Duration.ofSeconds(1));
	}

	private static void sleep(Duration duration) {
		try {
			Thread.sleep(duration);
		} catch (InterruptedException e) {
			// ignore
		}
	}
}
