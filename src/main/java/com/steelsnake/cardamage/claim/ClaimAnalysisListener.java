package com.steelsnake.cardamage.claim;

import java.time.Duration;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Component
class ClaimAnalysisListener {

	private static final Logger logger = LoggerFactory.getLogger(ClaimAnalysisListener.class);

	private final ClaimService claimService;
	private final Duration processingRetryInterval;

	ClaimAnalysisListener(
			ClaimService claimService,
			@Value("${app.kafka.analysis-retry-interval}") Duration processingRetryInterval) {
		this.claimService = claimService;
		this.processingRetryInterval = processingRetryInterval;
	}

	@KafkaListener(topics = DamageAnalysisRequested.TOPIC)
	Mono<Void> onAnalysisRequested(@Payload DamageAnalysisRequested event) {
		// валидация остаётся внутри Mono, чтобы ack контролировал контейнер
		return Mono.defer(() -> {
			if (event.claimId() == null) {
				logger.warn("Ignoring analysis request without a claim id");
				return Mono.empty();
			}
			if (event.version() != DamageAnalysisRequested.VERSION) {
				logger.warn(
						"Ignoring analysis request for claim {} with unsupported version {}",
						event.claimId(), event.version());
				return Mono.empty();
			}
			// при каждой попытке заново выполняем весь переход
			return Mono.defer(() -> this.claimService.startAnalysis(event.claimId()))
					.retryWhen(processingRetry(event.claimId()));
		});
	}

	// валидная запись остаётся без ack, пока переход Stage 5 не завершится
	private Retry processingRetry(UUID claimId) {
		return Retry.from(signals -> signals.concatMap(signal -> {
			logger.warn(
					"Retrying Stage 5 transition for claim {} after processing failed",
					claimId, signal.failure());
			return Mono.delay(this.processingRetryInterval);
		}));
	}
}
