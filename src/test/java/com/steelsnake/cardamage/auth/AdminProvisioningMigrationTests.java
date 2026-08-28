package com.steelsnake.cardamage.auth;

import java.util.Map;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers(disabledWithoutDocker = true)
class AdminProvisioningMigrationTests {

	private static final String VALID_BCRYPT_HASH =
			"$2a$10$dXJ3SW6G7P50lGmMkkmwe.20cQQubK3.HZWzG3YB1tlRy.fqvM/BG";
	private static final String WEAK_BCRYPT_HASH =
			"$2a$04$dXJ3SW6G7P50lGmMkkmwe.20cQQubK3.HZWzG3YB1tlRy.fqvM/BG";
	private static final String EXCESSIVE_BCRYPT_HASH =
			"$2a$31$dXJ3SW6G7P50lGmMkkmwe.20cQQubK3.HZWzG3YB1tlRy.fqvM/BG";

	@Container
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18-alpine")
			.withDatabaseName("admin_provisioning_test");

	@Test
	void costTenBcryptHashMigratesSuccessfully() {
		assertThatCode(() -> migrate("valid_admin", "admin@test.local", VALID_BCRYPT_HASH))
				.doesNotThrowAnyException();
	}

	@Test
	void blankAdminEmailIsRejectedAndMigrationCanBeRetried() {
		assertThatThrownBy(() -> migrate("blank_email", "   ", VALID_BCRYPT_HASH))
				.isInstanceOf(FlywayException.class);

		assertThatCode(() -> migrate("blank_email", "admin@test.local", VALID_BCRYPT_HASH))
				.doesNotThrowAnyException();
	}

	@Test
	void malformedBcryptHashIsRejectedAndMigrationCanBeRetried() {
		assertThatThrownBy(() -> migrate("malformed_hash", "admin@test.local", "$2garbage"))
				.isInstanceOf(FlywayException.class);

		assertThatCode(() -> migrate("malformed_hash", "admin@test.local", VALID_BCRYPT_HASH))
				.doesNotThrowAnyException();
	}

	@Test
	void weakCostBcryptHashIsRejected() {
		assertThatThrownBy(() -> migrate("weak_hash", "admin@test.local", WEAK_BCRYPT_HASH))
				.isInstanceOf(FlywayException.class);
	}

	@Test
	void excessiveCostBcryptHashIsRejected() {
		assertThatThrownBy(() -> migrate("excessive_hash", "admin@test.local", EXCESSIVE_BCRYPT_HASH))
				.isInstanceOf(FlywayException.class);
	}

	private static void migrate(String schema, String email, String passwordHash) {
		Flyway.configure()
				.dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
				.schemas(schema)
				.defaultSchema(schema)
				.placeholders(Map.of(
						"admin-email", email,
						"admin-password-hash", passwordHash))
				.load()
				.migrate();
	}
}
