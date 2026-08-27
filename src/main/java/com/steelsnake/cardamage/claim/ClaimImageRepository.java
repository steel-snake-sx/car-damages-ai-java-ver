package com.steelsnake.cardamage.claim;

import java.util.UUID;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import reactor.core.publisher.Flux;

public interface ClaimImageRepository extends ReactiveCrudRepository<ClaimImage, UUID> {

	Flux<ClaimImage> findAllByClaimId(UUID claimId);
}
