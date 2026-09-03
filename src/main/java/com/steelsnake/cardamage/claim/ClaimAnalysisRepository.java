package com.steelsnake.cardamage.claim;

import java.util.UUID;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import reactor.core.publisher.Mono;

public interface ClaimAnalysisRepository extends ReactiveCrudRepository<ClaimAnalysis, UUID> {

	Mono<ClaimAnalysis> findByClaimId(UUID claimId);
}
