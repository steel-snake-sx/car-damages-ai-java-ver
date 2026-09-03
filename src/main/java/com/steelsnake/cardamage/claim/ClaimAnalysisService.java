package com.steelsnake.cardamage.claim;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Service
public class ClaimAnalysisService {

	private static final Duration ANALYSIS_LEASE_DURATION = Duration.ofMinutes(10);
	private static final Logger logger = LoggerFactory.getLogger(ClaimAnalysisService.class);

	private final ClaimRepository claimRepository;
	private final ClaimImageRepository claimImageRepository;
	private final ClaimAnalysisRepository claimAnalysisRepository;
	private final ClaimAnalysisFindingRepository claimAnalysisFindingRepository;
	private final ImageStorage imageStorage;
	private final DamageAnalyzer damageAnalyzer;
	private final TransactionalOperator transactionalOperator;
	private final int retryAttempts;
	private final Duration retryBackoff;
	private final Duration persistenceRetryInterval;

	ClaimAnalysisService(
			ClaimRepository claimRepository,
			ClaimImageRepository claimImageRepository,
			ClaimAnalysisRepository claimAnalysisRepository,
			ClaimAnalysisFindingRepository claimAnalysisFindingRepository,
			ImageStorage imageStorage,
			DamageAnalyzer damageAnalyzer,
			TransactionalOperator transactionalOperator,
			@Value("${app.ai.retry-attempts}") int retryAttempts,
			@Value("${app.ai.retry-backoff}") Duration retryBackoff,
			@Value("${app.kafka.analysis-retry-interval}") Duration persistenceRetryInterval) {
		this.claimRepository = claimRepository;
		this.claimImageRepository = claimImageRepository;
		this.claimAnalysisRepository = claimAnalysisRepository;
		this.claimAnalysisFindingRepository = claimAnalysisFindingRepository;
		this.imageStorage = imageStorage;
		this.damageAnalyzer = damageAnalyzer;
		this.transactionalOperator = transactionalOperator;
		this.retryAttempts = retryAttempts;
		this.retryBackoff = retryBackoff;
		this.persistenceRetryInterval = persistenceRetryInterval;
	}

	public Mono<Void> analyze(UUID claimId) {
		UUID ownerToken = UUID.randomUUID();
		return acquireOwnership(claimId, ownerToken)
				.flatMap(token -> runAnalysis(claimId, token));
	}

	private Mono<UUID> acquireOwnership(UUID claimId, UUID ownerToken) {
		Instant now = Instant.now();
		return this.claimRepository.acquireAnalysis(
				claimId, ownerToken, now.plus(ANALYSIS_LEASE_DURATION), now, now)
				.flatMap(updated -> {
					if (updated == 1) {
						return Mono.just(ownerToken);
					}
					return this.claimRepository.findById(claimId)
							.flatMap(claim -> switch (claim.status()) {
								case ANALYZED, ANALYSIS_FAILED -> Mono.<UUID>empty();
								case ANALYSIS_PENDING, ANALYZING -> Mono.delay(this.persistenceRetryInterval)
										.then(acquireOwnership(claimId, ownerToken));
							});
				})
				.doOnSuccess(token -> {
					if (token == null) {
						logger.info("Skipping analysis for claim {} already in terminal status", claimId);
					}
				});
	}

	private Mono<Void> runAnalysis(UUID claimId, UUID ownerToken) {
		// outcome кэшируется, чтобы ретрай БД не запускал AI заново
		Mono<AnalysisOutcome> outcome = loadImages(claimId)
				.flatMap(images -> Mono.defer(() -> this.damageAnalyzer.analyze(images))
						.retryWhen(analyzerRetry(claimId))
						.map(AnalysisOutcome::success)
						.onErrorResume(DamageAnalysisException.class,
								error -> Mono.just(AnalysisOutcome.failure(error))))
				.cache();
		return outcome
				.flatMap(result -> persistOutcome(claimId, ownerToken, result));
	}

	private Mono<List<AnalysisImage>> loadImages(UUID claimId) {
		return this.claimImageRepository.findAllByClaimId(claimId)
				.concatMap(image -> this.imageStorage.readImage(image.storagePath())
						.map(content -> new AnalysisImage(image.contentType(), content)))
				.collectList()
				.flatMap(images -> images.isEmpty()
						? Mono.error(new IllegalStateException("Claim " + claimId + " has no stored images"))
						: Mono.just(images));
	}

	// временную недоступность AI ретраим ограниченно, остальные исходы сохраняем сразу
	private Retry analyzerRetry(UUID claimId) {
		return Retry.backoff(this.retryAttempts, this.retryBackoff)
				.filter(error -> error instanceof DamageAnalysisException failure
						&& failure.reason().retryable())
				.doBeforeRetry(signal -> logger.warn(
						"Retrying damage analysis for claim {} after attempt {}",
						claimId, signal.totalRetries() + 1, signal.failure()))
				.onRetryExhaustedThrow((specification, signal) -> signal.failure());
	}

	private Mono<Void> persistOutcome(UUID claimId, UUID ownerToken, AnalysisOutcome outcome) {
		return Mono.defer(() -> this.claimRepository.findById(claimId))
				.flatMap(claim -> switch (claim.status()) {
					case ANALYZED, ANALYSIS_FAILED -> Mono.<Void>empty();
					case ANALYSIS_PENDING, ANALYZING -> outcome.analysis() != null
							? saveResult(claimId, ownerToken, outcome.analysis())
							: saveFailure(claimId, ownerToken, outcome.failure());
				})
				.switchIfEmpty(Mono.empty())
				.retryWhen(persistenceRetry(claimId));
	}

	private Retry persistenceRetry(UUID claimId) {
		return Retry.from(signals -> signals.concatMap(signal -> {
			if (signal.failure() instanceof OwnershipLostException) {
				return Mono.error(signal.failure());
			}
			logger.warn(
					"Retrying persistence for claim {} after analysis outcome could not be stored",
					claimId, signal.failure());
			return Mono.delay(this.persistenceRetryInterval);
		}));
	}

	private Mono<Void> saveResult(UUID claimId, UUID ownerToken, DamageAnalysis analysis) {
		Mono<Void> save = Mono.defer(() -> {
			Instant now = Instant.now();
			return this.claimAnalysisRepository.save(new ClaimAnalysis(
							null, claimId, analysis.carDetected(), analysis.summary(),
							analysis.confidence(), now))
					.thenMany(saveFindings(claimId, analysis.findings()))
					.then(Mono.defer(() -> this.claimRepository.completeAnalysis(
							claimId, ownerToken, now, now)))
					.flatMap(updated -> updated == 1
							? Mono.<Void>empty()
							: Mono.error(new OwnershipLostException(
									"Claim " + claimId + " no longer belongs to this analysis worker")));
		});
		return this.transactionalOperator.transactional(save);
	}

	private Flux<ClaimAnalysisFinding> saveFindings(UUID claimId, List<DamageFinding> findings) {
		return Flux.range(0, findings.size())
				.concatMap(position -> {
					DamageFinding finding = findings.get(position);
					return this.claimAnalysisFindingRepository.save(new ClaimAnalysisFinding(
							null,
							claimId,
							position.shortValue(),
							finding.partName(),
							finding.description(),
							finding.severity(),
							finding.confidence()));
				});
	}

	private Mono<Void> saveFailure(UUID claimId, UUID ownerToken, DamageAnalysisException error) {
		logger.warn("Marking claim {} as ANALYSIS_FAILED because of {}", claimId, error.reason(), error);
		Instant now = Instant.now();
		return this.claimRepository.failAnalysis(claimId, ownerToken, error.reason(), now, now)
				.flatMap(updated -> updated == 1
						? Mono.<Void>empty()
						: Mono.error(new OwnershipLostException(
								"Claim " + claimId + " no longer belongs to this analysis worker")));
	}

	private record AnalysisOutcome(DamageAnalysis analysis, DamageAnalysisException failure) {

		private static AnalysisOutcome success(DamageAnalysis analysis) {
			return new AnalysisOutcome(analysis, null);
		}

		private static AnalysisOutcome failure(DamageAnalysisException failure) {
			return new AnalysisOutcome(null, failure);
		}
	}

	@SuppressWarnings("serial")
	private static final class OwnershipLostException extends IllegalStateException {

		private OwnershipLostException(String message) {
			super(message);
		}
	}
}
