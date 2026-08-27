package com.steelsnake.cardamage.claim;

import java.util.UUID;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface ClaimRepository extends ReactiveCrudRepository<Claim, UUID> {
}
