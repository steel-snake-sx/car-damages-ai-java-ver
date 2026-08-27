package com.steelsnake.cardamage.claim;

import java.time.Instant;
import java.util.UUID;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("claim_images")
public record ClaimImage(
		@Id UUID id,
		UUID claimId,
		String storagePath,
		String originalFilename,
		String contentType,
		long sizeBytes,
		Instant createdAt) {
}
