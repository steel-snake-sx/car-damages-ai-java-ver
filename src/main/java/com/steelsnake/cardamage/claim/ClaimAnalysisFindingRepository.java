package com.steelsnake.cardamage.claim;

import java.util.UUID;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ClaimAnalysisFindingRepository
		extends ReactiveCrudRepository<ClaimAnalysisFinding, UUID> {

	Flux<ClaimAnalysisFinding> findAllByClaimIdOrderByPosition(UUID claimId);
}
