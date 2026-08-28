package com.steelsnake.cardamage.claim;

import java.time.Instant;
import java.util.UUID;

public record AdminClaimDetails(
		UUID id,
		String carBrand,
		String carModel,
		int carYear,
		ClaimStatus status,
		Instant createdAt,
		Instant updatedAt) {

	static AdminClaimDetails from(Claim claim) {
		return new AdminClaimDetails(
				claim.id(),
				claim.carBrand(),
				claim.carModel(),
				claim.carYear(),
				claim.status(),
				claim.createdAt(),
				claim.updatedAt());
	}
}
