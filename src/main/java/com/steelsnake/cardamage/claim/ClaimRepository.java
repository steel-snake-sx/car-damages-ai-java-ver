package com.steelsnake.cardamage.claim;

import java.util.UUID;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import reactor.core.publisher.Flux;

public interface ClaimRepository extends ReactiveCrudRepository<Claim, UUID> {

	Flux<Claim> findAllByOrderByCreatedAtDesc();
}
