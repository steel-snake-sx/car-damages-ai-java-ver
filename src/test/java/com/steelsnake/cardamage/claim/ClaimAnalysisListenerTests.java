package com.steelsnake.cardamage.claim;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.dao.DataIntegrityViolationException;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ClaimAnalysisListenerTests {

	private ClaimService claimService;
	private ClaimAnalysisListener listener;

	@BeforeEach
	void configureListener() {
		this.claimService = mock(ClaimService.class);
		this.listener = new ClaimAnalysisListener(this.claimService, Duration.ofMillis(10));
	}

	@Test
	void supportedEventStartsAnalysisOnce() {
		UUID claimId = UUID.randomUUID();
		when(this.claimService.startAnalysis(claimId)).thenReturn(Mono.empty());

		StepVerifier.create(this.listener.onAnalysisRequested(DamageAnalysisRequested.of(claimId)))
				.expectComplete()
				.verify(Duration.ofSeconds(1));

		verify(this.claimService, times(1)).startAnalysis(claimId);
	}

	@Test
	void permanentProcessingFailureIsRetriedUntilTheTransitionSucceeds() {
		UUID claimId = UUID.randomUUID();
		when(this.claimService.startAnalysis(claimId))
				.thenReturn(Mono.error(new DataIntegrityViolationException("invalid database state")))
				.thenReturn(Mono.error(new DataIntegrityViolationException("invalid database state")))
				.thenReturn(Mono.empty());

		StepVerifier.withVirtualTime(
				() -> this.listener.onAnalysisRequested(DamageAnalysisRequested.of(claimId)))
				.thenAwait(Duration.ofMillis(20))
				.expectComplete()
				.verify(Duration.ofSeconds(1));

		verify(this.claimService, times(3)).startAnalysis(claimId);
	}

	@Test
	void applicationFailureIsAlsoRetriedUntilTheTransitionSucceeds() {
		UUID claimId = UUID.randomUUID();
		when(this.claimService.startAnalysis(claimId))
				.thenReturn(Mono.error(new IllegalStateException("unexpected failure")))
				.thenReturn(Mono.empty());

		StepVerifier.withVirtualTime(
				() -> this.listener.onAnalysisRequested(DamageAnalysisRequested.of(claimId)))
				.thenAwait(Duration.ofMillis(10))
				.expectComplete()
				.verify(Duration.ofSeconds(1));

		verify(this.claimService, times(2)).startAnalysis(claimId);
	}

	@Test
	void unsupportedVersionCompletesWithoutStartingAnalysis() {
		StepVerifier.create(this.listener.onAnalysisRequested(
				new DamageAnalysisRequested(99, UUID.randomUUID())))
				.expectComplete()
				.verify(Duration.ofSeconds(1));

		verifyNoInteractions(this.claimService);
	}

	@Test
	void eventWithoutClaimIdCompletesWithoutStartingAnalysis() {
		StepVerifier.create(this.listener.onAnalysisRequested(
				new DamageAnalysisRequested(DamageAnalysisRequested.VERSION, null)))
				.expectComplete()
				.verify(Duration.ofSeconds(1));

		verifyNoInteractions(this.claimService);
	}

	@Test
	void listenerDoesNotSubscribeBeforeItIsReturned() {
		UUID claimId = UUID.randomUUID();
		when(this.claimService.startAnalysis(any(UUID.class))).thenReturn(Mono.empty());

		this.listener.onAnalysisRequested(DamageAnalysisRequested.of(claimId));

		// подпиской и ack управляет контейнер
		verifyNoInteractions(this.claimService);
	}
}
