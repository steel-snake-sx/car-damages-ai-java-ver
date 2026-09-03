package com.steelsnake.cardamage.claim;

import java.time.Instant;
import java.util.UUID;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("claim_analyses")
public record ClaimAnalysis(
		@Id UUID id,
		UUID claimId,
		boolean carDetected,
		String summary,
		double confidence,
		Instant createdAt) {
}
