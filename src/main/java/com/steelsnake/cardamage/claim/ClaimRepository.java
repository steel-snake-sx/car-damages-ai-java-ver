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

	// проверка статуса защищает от повторного перехода при повторной доставке
	@Modifying
	@Query("UPDATE claims SET status = 'ANALYZING', updated_at = :updatedAt "
			+ "WHERE id = :id AND status = 'ANALYSIS_PENDING'")
	Mono<Long> startAnalysis(UUID id, Instant updatedAt);
}
