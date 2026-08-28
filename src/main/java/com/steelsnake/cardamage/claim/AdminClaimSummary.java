package com.steelsnake.cardamage.claim;

import java.time.Instant;
import java.util.UUID;

public record AdminClaimSummary(
		UUID id,
		String carBrand,
		String carModel,
		int carYear,
		ClaimStatus status,
		Instant createdAt) {

	static AdminClaimSummary from(Claim claim) {
		return new AdminClaimSummary(
				claim.id(),
				claim.carBrand(),
				claim.carModel(),
				claim.carYear(),
				claim.status(),
				claim.createdAt());
	}
}
