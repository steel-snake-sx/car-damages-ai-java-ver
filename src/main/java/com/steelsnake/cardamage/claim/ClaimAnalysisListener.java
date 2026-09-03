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

	private final ClaimAnalysisService claimAnalysisService;
	private final Duration processingRetryInterval;

	ClaimAnalysisListener(
			ClaimAnalysisService claimAnalysisService,
			@Value("${app.kafka.analysis-retry-interval}") Duration processingRetryInterval) {
		this.claimAnalysisService = claimAnalysisService;
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
			// ошибка до сохранения исхода или потеря владения оставляет сообщение без подтверждения
			return Mono.defer(() -> this.claimAnalysisService.analyze(event.claimId()))
					.retryWhen(processingRetry(event.claimId()));
		});
	}

	// ошибки сохранения ретраятся внутри сервиса, остальные ошибки повторяют доставку
	private Retry processingRetry(UUID claimId) {
		return Retry.from(signals -> signals.concatMap(signal -> {
			logger.warn(
					"Retrying analysis processing for claim {} after persistence failed",
					claimId, signal.failure());
			return Mono.delay(this.processingRetryInterval);
		}));
	}
}
