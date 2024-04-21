package com.example.kafkaretry.exception;

import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

public sealed interface ListenerExceptions {

	BackOff ZERO_BACKOFF = new FixedBackOff(0, 0);

	BackOff getBackOff();

	abstract non-sealed class DelayedRetry extends RuntimeException implements ListenerExceptions {

		private final int retryCount;
		private final long retryIntervalMillis;

		public DelayedRetry(String message, int retryCount, long retryIntervalMillis) {
			super(message);
			this.retryCount = retryCount;
			this.retryIntervalMillis = retryIntervalMillis;
		}

		@Override
		public BackOff getBackOff() {
			return new FixedBackOff(retryIntervalMillis, retryCount);
		}
	}

	abstract non-sealed class ImmediateRetry extends RuntimeException implements ListenerExceptions {

		private final int retryCount;

		public ImmediateRetry(String message, int retryCount) {
			super(message);
			this.retryCount = retryCount;
		}

		@Override
		public BackOff getBackOff() {
			return new FixedBackOff(0, retryCount);
		}
	}

	abstract non-sealed class ImmediatePublish extends RuntimeException implements ListenerExceptions {

		public ImmediatePublish(String message) {
			super(message);
		}

		@Override
		public BackOff getBackOff() {
			return ZERO_BACKOFF;
		}
	}

	abstract non-sealed class NoOp extends RuntimeException implements ListenerExceptions {
		@Override
		public BackOff getBackOff() {
			return ZERO_BACKOFF;
		}
	}
}
