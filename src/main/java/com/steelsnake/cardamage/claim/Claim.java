package com.steelsnake.cardamage.claim;

import java.time.Instant;
import java.util.UUID;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("claims")
public record Claim(
		@Id UUID id,
		String carBrand,
		String carModel,
		int carYear,
		ClaimStatus status,
		// заполняется только вместе с ANALYSIS_FAILED
		AnalysisFailureReason analysisFailureReason,
		UUID analysisOwnerToken,
		Instant analysisLeaseUntil,
		Instant createdAt,
		Instant updatedAt) {

	static Claim pending(String carBrand, String carModel, int carYear, Instant now) {
		return new Claim(null, carBrand, carModel, carYear, ClaimStatus.ANALYSIS_PENDING,
				null, null, null, now, now);
	}
}
