package com.steelsnake.cardamage.auth;

import java.time.Instant;
import java.util.UUID;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("admin_users")
public record AdminUser(
		@Id UUID id,
		String email,
		String passwordHash,
		Instant createdAt) {
}
