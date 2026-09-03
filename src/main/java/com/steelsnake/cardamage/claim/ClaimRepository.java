package com.steelsnake.cardamage.claim;

import java.time.Instant;
import java.util.UUID;

import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ClaimRepository extends ReactiveCrudRepository<Claim, UUID> {

	Flux<Claim> findAllByOrderByCreatedAtDesc();

	// token и lease меняются вместе со статусом в одном условном обновлении
	@Modifying
	@Query("UPDATE claims SET status = 'ANALYZING', analysis_owner_token = :ownerToken, "
			+ "analysis_lease_until = :leaseUntil, updated_at = :updatedAt "
			+ "WHERE id = :id AND (status = 'ANALYSIS_PENDING' "
			+ "OR (status = 'ANALYZING' AND (analysis_lease_until IS NULL OR analysis_lease_until <= :now)))")
	Mono<Long> acquireAnalysis(UUID id, UUID ownerToken, Instant leaseUntil, Instant now, Instant updatedAt);

	@Modifying
	@Query("UPDATE claims SET status = 'ANALYZED', analysis_failure_reason = NULL, "
			+ "analysis_owner_token = NULL, analysis_lease_until = NULL, updated_at = :updatedAt "
			+ "WHERE id = :id AND status = 'ANALYZING' AND analysis_owner_token = :ownerToken "
			+ "AND analysis_lease_until > :now")
	Mono<Long> completeAnalysis(UUID id, UUID ownerToken, Instant now, Instant updatedAt);

	@Modifying
	@Query("UPDATE claims SET status = 'ANALYSIS_FAILED', analysis_failure_reason = :reason, "
			+ "analysis_owner_token = NULL, analysis_lease_until = NULL, updated_at = :updatedAt "
			+ "WHERE id = :id AND status = 'ANALYZING' AND analysis_owner_token = :ownerToken "
			+ "AND analysis_lease_until > :now")
	Mono<Long> failAnalysis(UUID id, UUID ownerToken, AnalysisFailureReason reason, Instant now, Instant updatedAt);
}
