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
		Instant createdAt,
		Instant updatedAt) {
}
