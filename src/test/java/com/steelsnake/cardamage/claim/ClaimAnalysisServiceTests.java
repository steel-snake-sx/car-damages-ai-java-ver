package com.steelsnake.cardamage.claim;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.reactive.AbstractReactiveTransactionManager;
import org.springframework.transaction.reactive.GenericReactiveTransaction;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.reactive.TransactionalOperator;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ClaimAnalysisServiceTests {

	private static final UUID CLAIM_ID = UUID.randomUUID();

	@TempDir
	Path imageDirectory;

	private ClaimRepository claimRepository;
	private ClaimImageRepository claimImageRepository;
	private ClaimAnalysisRepository analysisRepository;
	private ClaimAnalysisFindingRepository findingRepository;
	private DamageAnalyzer damageAnalyzer;
	private TestTransactionManager transactionManager;
	private ClaimAnalysisService service;

	@BeforeEach
	void configureService() throws IOException {
		this.claimRepository = mock(ClaimRepository.class);
		this.claimImageRepository = mock(ClaimImageRepository.class);
		this.analysisRepository = mock(ClaimAnalysisRepository.class);
		this.findingRepository = mock(ClaimAnalysisFindingRepository.class);
		this.damageAnalyzer = mock(DamageAnalyzer.class);
		this.transactionManager = new TestTransactionManager();
		this.service = new ClaimAnalysisService(
				this.claimRepository,
				this.claimImageRepository,
				this.analysisRepository,
				this.findingRepository,
				new ImageStorage(this.imageDirectory.toString()),
				this.damageAnalyzer,
				TransactionalOperator.create(this.transactionManager),
				2,
				Duration.ofMillis(1),
				Duration.ofMillis(1));

		Files.createDirectories(this.imageDirectory.resolve(CLAIM_ID.toString()));
		Files.write(this.imageDirectory.resolve(CLAIM_ID + "/damage.png"), new byte[] {1, 2, 3});
		when(this.claimImageRepository.findAllByClaimId(CLAIM_ID)).thenReturn(Flux.just(new ClaimImage(
				UUID.randomUUID(), CLAIM_ID, CLAIM_ID + "/damage.png", "damage.png",
				"image/png", 3, Instant.now())));
	}

	@Test
	void claimedWorkIsAnalyzedAndPersistedAsAnalyzed() {
		ownershipAcquired();
		analyzerReturns(analysis());
		persistenceSucceeds(1L);

		StepVerifier.create(this.service.analyze(CLAIM_ID))
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		verify(this.damageAnalyzer, times(1)).analyze(anyList());
		verify(this.analysisRepository).save(any(ClaimAnalysis.class));
		verify(this.findingRepository, times(2)).save(any(ClaimAnalysisFinding.class));
		verify(this.claimRepository).completeAnalysis(
				eq(CLAIM_ID), any(UUID.class), any(Instant.class), any(Instant.class));
		verify(this.claimRepository, never()).failAnalysis(any(), any(), any(), any(), any());
		assertThat(this.transactionManager.commits).isEqualTo(1);
	}

	@Test
	void activeOwnerIsWaitedForWithoutCallingAnalyzerOrSavingResult() {
		Instant leaseUntil = Instant.now().plusSeconds(30);
		when(this.claimRepository.acquireAnalysis(
				eq(CLAIM_ID), any(UUID.class), any(Instant.class), any(Instant.class), any(Instant.class)))
				.thenReturn(Mono.just(0L), Mono.just(0L));
		when(this.claimRepository.findById(CLAIM_ID)).thenReturn(
				Mono.just(claim(ClaimStatus.ANALYZING, UUID.randomUUID(), leaseUntil)),
				Mono.just(claim(ClaimStatus.ANALYZED, null, null)));

		StepVerifier.create(this.service.analyze(CLAIM_ID))
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		verify(this.claimRepository, times(2)).acquireAnalysis(
				eq(CLAIM_ID), any(UUID.class), any(Instant.class), any(Instant.class), any(Instant.class));
		verifyNoInteractions(this.damageAnalyzer, this.analysisRepository, this.findingRepository);
	}

	@Test
	void staleWorkerCannotCommitAfterOwnershipWasReplaced() {
		ownershipAcquired();
		analyzerReturns(analysis());
		persistenceSucceeds(0L);

		StepVerifier.create(this.service.analyze(CLAIM_ID))
				.expectError(IllegalStateException.class)
				.verify(Duration.ofSeconds(5));

		verify(this.damageAnalyzer).analyze(anyList());
		verify(this.claimRepository).completeAnalysis(
				eq(CLAIM_ID), any(UUID.class), any(Instant.class), any(Instant.class));
		assertThat(this.transactionManager.rollbacks).isEqualTo(1);
	}

	@Test
	void expiredLeaseCanBeResumedByANewWorker() {
		ownershipAcquired();
		analyzerReturns(analysis());
		persistenceSucceeds(1L);

		StepVerifier.create(this.service.analyze(CLAIM_ID))
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		verify(this.claimRepository).acquireAnalysis(
				eq(CLAIM_ID), any(UUID.class), any(Instant.class), any(Instant.class), any(Instant.class));
		verify(this.damageAnalyzer).analyze(anyList());
		verify(this.claimRepository).completeAnalysis(
				eq(CLAIM_ID), any(UUID.class), any(Instant.class), any(Instant.class));
	}

	@Test
	void terminalClaimDoesNotReceiveNewOwnership() {
		ownershipNotAcquired();
		when(this.claimRepository.findById(CLAIM_ID))
				.thenReturn(Mono.just(claim(ClaimStatus.ANALYSIS_FAILED, null, null)));

		StepVerifier.create(this.service.analyze(CLAIM_ID))
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		verify(this.claimRepository).acquireAnalysis(
				eq(CLAIM_ID), any(UUID.class), any(Instant.class), any(Instant.class), any(Instant.class));
		verifyNoInteractions(this.damageAnalyzer, this.analysisRepository, this.findingRepository);
	}

	@Test
	void analyzedClaimDoesNotReceiveNewOwnership() {
		ownershipNotAcquired();
		when(this.claimRepository.findById(CLAIM_ID))
				.thenReturn(Mono.just(claim(ClaimStatus.ANALYZED, null, null)));

		StepVerifier.create(this.service.analyze(CLAIM_ID))
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		verifyNoInteractions(this.damageAnalyzer, this.analysisRepository, this.findingRepository);
	}

	@Test
	void temporaryAnalyzerFailureIsRetriedUntilItSucceeds() {
		ownershipAcquired();
		AtomicInteger attempts = new AtomicInteger();
		when(this.damageAnalyzer.analyze(anyList())).thenAnswer(invocation -> {
			if (attempts.incrementAndGet() <= 2) {
				return Mono.error(DamageAnalysisException.unavailable("AI is busy", null));
			}
			return Mono.just(analysis());
		});
		persistenceSucceeds(1L);

		StepVerifier.create(this.service.analyze(CLAIM_ID))
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		assertThat(attempts.get()).isEqualTo(3);
		verify(this.claimRepository).completeAnalysis(
				eq(CLAIM_ID), any(UUID.class), any(Instant.class), any(Instant.class));
	}

	@Test
	void exhaustedAnalyzerRetriesRecordAnalysisFailed() {
		ownershipAcquired();
		when(this.damageAnalyzer.analyze(anyList()))
				.thenReturn(Mono.error(DamageAnalysisException.unavailable("AI is down", null)));
		when(this.claimRepository.failAnalysis(
				eq(CLAIM_ID), any(UUID.class), eq(AnalysisFailureReason.AI_UNAVAILABLE),
				any(Instant.class), any(Instant.class)))
				.thenReturn(Mono.just(1L));

		StepVerifier.create(this.service.analyze(CLAIM_ID))
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		verify(this.damageAnalyzer, times(3)).analyze(anyList());
		verify(this.claimRepository).failAnalysis(
				eq(CLAIM_ID), any(UUID.class), eq(AnalysisFailureReason.AI_UNAVAILABLE),
				any(Instant.class), any(Instant.class));
		verifyNoInteractions(this.analysisRepository, this.findingRepository);
	}

	@Test
	void rejectedRequestIsNotRetried() {
		ownershipAcquired();
		when(this.damageAnalyzer.analyze(anyList()))
				.thenReturn(Mono.error(DamageAnalysisException.requestRejected("bad request", null)));
		when(this.claimRepository.failAnalysis(
				eq(CLAIM_ID), any(UUID.class), eq(AnalysisFailureReason.AI_REQUEST_REJECTED),
				any(Instant.class), any(Instant.class)))
				.thenReturn(Mono.just(1L));

		StepVerifier.create(this.service.analyze(CLAIM_ID))
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		verify(this.damageAnalyzer, times(1)).analyze(anyList());
		verify(this.claimRepository).failAnalysis(
				eq(CLAIM_ID), any(UUID.class), eq(AnalysisFailureReason.AI_REQUEST_REJECTED),
				any(Instant.class), any(Instant.class));
	}

	@Test
	void persistenceRetryDoesNotCallAnalyzerAgain() {
		ownershipAcquired();
		analyzerReturns(analysis());
		when(this.analysisRepository.save(any(ClaimAnalysis.class)))
				.thenReturn(Mono.error(new TransientDataAccessResourceException("database is down")))
				.thenReturn(Mono.error(new TransientDataAccessResourceException("database is still down")))
				.thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
		persistenceFinalizationSucceeds(1L);

		StepVerifier.create(this.service.analyze(CLAIM_ID))
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		verify(this.damageAnalyzer, times(1)).analyze(anyList());
		verify(this.analysisRepository, times(3)).save(any(ClaimAnalysis.class));
		verify(this.claimRepository).completeAnalysis(
				eq(CLAIM_ID), any(UUID.class), any(Instant.class), any(Instant.class));
	}

	@Test
	void terminalFailurePersistenceRetryDoesNotCallAnalyzerAgain() {
		ownershipAcquired();
		when(this.damageAnalyzer.analyze(anyList()))
				.thenReturn(Mono.error(DamageAnalysisException.requestRejected("bad request", null)));
		when(this.claimRepository.failAnalysis(
				eq(CLAIM_ID), any(UUID.class), eq(AnalysisFailureReason.AI_REQUEST_REJECTED),
				any(Instant.class), any(Instant.class)))
				.thenReturn(Mono.error(new TransientDataAccessResourceException("database is down")))
				.thenReturn(Mono.error(new TransientDataAccessResourceException("database is still down")))
				.thenReturn(Mono.just(1L));

		StepVerifier.create(this.service.analyze(CLAIM_ID))
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		verify(this.damageAnalyzer, times(1)).analyze(anyList());
		verify(this.claimRepository, times(3)).failAnalysis(
				eq(CLAIM_ID), any(UUID.class), eq(AnalysisFailureReason.AI_REQUEST_REJECTED),
				any(Instant.class), any(Instant.class));
	}

	@Test
	void missingFinalTransitionRollsBackTheResult() {
		ownershipAcquired();
		analyzerReturns(analysis());
		persistenceSucceeds(0L);

		StepVerifier.create(this.service.analyze(CLAIM_ID))
				.expectError(IllegalStateException.class)
				.verify(Duration.ofSeconds(5));

		assertThat(this.transactionManager.rollbacks).isEqualTo(1);
	}

	@Test
	void missingClaimCompletesWithoutCallingTheAnalyzer() {
		ownershipNotAcquired();
		when(this.claimRepository.findById(CLAIM_ID)).thenReturn(Mono.empty());

		StepVerifier.create(this.service.analyze(CLAIM_ID))
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		verifyNoInteractions(this.damageAnalyzer);
	}

	private void ownershipAcquired() {
		when(this.claimRepository.acquireAnalysis(
				eq(CLAIM_ID), any(UUID.class), any(Instant.class), any(Instant.class), any(Instant.class)))
				.thenReturn(Mono.just(1L));
		when(this.claimRepository.findById(CLAIM_ID))
				.thenReturn(Mono.just(claim(ClaimStatus.ANALYZING, null, Instant.now().plusSeconds(600))));
	}

	private void ownershipNotAcquired() {
		when(this.claimRepository.acquireAnalysis(
				eq(CLAIM_ID), any(UUID.class), any(Instant.class), any(Instant.class), any(Instant.class)))
				.thenReturn(Mono.just(0L));
	}

	private void analyzerReturns(DamageAnalysis analysis) {
		when(this.damageAnalyzer.analyze(anyList())).thenReturn(Mono.just(analysis));
	}

	private void persistenceSucceeds(long finalTransitionRows) {
		when(this.analysisRepository.save(any(ClaimAnalysis.class)))
				.thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
		persistenceFinalizationSucceeds(finalTransitionRows);
	}

	private void persistenceFinalizationSucceeds(long finalTransitionRows) {
		when(this.findingRepository.save(any(ClaimAnalysisFinding.class)))
				.thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
		when(this.claimRepository.completeAnalysis(
				eq(CLAIM_ID), any(UUID.class), any(Instant.class), any(Instant.class)))
				.thenReturn(Mono.just(finalTransitionRows));
	}

	private static Claim claim(ClaimStatus status, UUID ownerToken, Instant leaseUntil) {
		Instant now = Instant.now();
		return new Claim(CLAIM_ID, "Toyota", "Camry", 2022, status, null,
				ownerToken, leaseUntil, now, now);
	}

	private static DamageAnalysis analysis() {
		return new DamageAnalysis(
				true,
				"Повреждена передняя часть",
				0.9,
				List.of(
						new DamageFinding("Передний бампер", "Царапины", DamageSeverity.MEDIUM, 0.9),
						new DamageFinding("Левое крыло", "Вмятина", DamageSeverity.LOW, 0.8)));
	}

	@SuppressWarnings("serial")
	private static final class TestTransactionManager extends AbstractReactiveTransactionManager {

		private int commits;
		private int rollbacks;

		@Override
		protected Object doGetTransaction(TransactionSynchronizationManager manager)
				throws TransactionException {
			return new Object();
		}

		@Override
		protected Mono<Void> doBegin(
				TransactionSynchronizationManager manager,
				Object transaction,
				TransactionDefinition definition) throws TransactionException {
			return Mono.empty();
		}

		@Override
		protected Mono<Void> doCommit(
				TransactionSynchronizationManager manager,
				GenericReactiveTransaction status) throws TransactionException {
			this.commits++;
			return Mono.empty();
		}

		@Override
		protected Mono<Void> doRollback(
				TransactionSynchronizationManager manager,
				GenericReactiveTransaction status) throws TransactionException {
			this.rollbacks++;
			return Mono.empty();
		}
	}
}
