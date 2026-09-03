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

	private ClaimAnalysisService claimAnalysisService;
	private ClaimAnalysisListener listener;

	@BeforeEach
	void configureListener() {
		this.claimAnalysisService = mock(ClaimAnalysisService.class);
		this.listener = new ClaimAnalysisListener(this.claimAnalysisService, Duration.ofMillis(10));
	}

	@Test
	void supportedEventStartsAnalysisOnce() {
		UUID claimId = UUID.randomUUID();
		when(this.claimAnalysisService.analyze(claimId)).thenReturn(Mono.empty());

		StepVerifier.create(this.listener.onAnalysisRequested(DamageAnalysisRequested.of(claimId)))
				.expectComplete()
				.verify(Duration.ofSeconds(1));

		verify(this.claimAnalysisService, times(1)).analyze(claimId);
	}

	@Test
	void permanentProcessingFailureIsRetriedUntilTheTransitionSucceeds() {
		UUID claimId = UUID.randomUUID();
		when(this.claimAnalysisService.analyze(claimId))
				.thenReturn(Mono.error(new DataIntegrityViolationException("invalid database state")))
				.thenReturn(Mono.error(new DataIntegrityViolationException("invalid database state")))
				.thenReturn(Mono.empty());

		StepVerifier.withVirtualTime(
				() -> this.listener.onAnalysisRequested(DamageAnalysisRequested.of(claimId)))
				.thenAwait(Duration.ofMillis(20))
				.expectComplete()
				.verify(Duration.ofSeconds(1));

		verify(this.claimAnalysisService, times(3)).analyze(claimId);
	}

	@Test
	void applicationFailureIsAlsoRetriedUntilTheTransitionSucceeds() {
		UUID claimId = UUID.randomUUID();
		when(this.claimAnalysisService.analyze(claimId))
				.thenReturn(Mono.error(new IllegalStateException("unexpected failure")))
				.thenReturn(Mono.empty());

		StepVerifier.withVirtualTime(
				() -> this.listener.onAnalysisRequested(DamageAnalysisRequested.of(claimId)))
				.thenAwait(Duration.ofMillis(10))
				.expectComplete()
				.verify(Duration.ofSeconds(1));

		verify(this.claimAnalysisService, times(2)).analyze(claimId);
	}

	@Test
	void unsupportedVersionCompletesWithoutStartingAnalysis() {
		StepVerifier.create(this.listener.onAnalysisRequested(
				new DamageAnalysisRequested(99, UUID.randomUUID())))
				.expectComplete()
				.verify(Duration.ofSeconds(1));

		verifyNoInteractions(this.claimAnalysisService);
	}

	@Test
	void eventWithoutClaimIdCompletesWithoutStartingAnalysis() {
		StepVerifier.create(this.listener.onAnalysisRequested(
				new DamageAnalysisRequested(DamageAnalysisRequested.VERSION, null)))
				.expectComplete()
				.verify(Duration.ofSeconds(1));

		verifyNoInteractions(this.claimAnalysisService);
	}

	@Test
	void listenerDoesNotSubscribeBeforeItIsReturned() {
		UUID claimId = UUID.randomUUID();
		when(this.claimAnalysisService.analyze(any(UUID.class))).thenReturn(Mono.empty());

		this.listener.onAnalysisRequested(DamageAnalysisRequested.of(claimId));

		verifyNoInteractions(this.claimAnalysisService);
	}
}
