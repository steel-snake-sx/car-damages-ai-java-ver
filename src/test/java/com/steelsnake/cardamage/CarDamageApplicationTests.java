package com.steelsnake.cardamage;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;

import com.steelsnake.cardamage.claim.Claim;
import com.steelsnake.cardamage.claim.ClaimImage;
import com.steelsnake.cardamage.claim.ClaimImageRepository;
import com.steelsnake.cardamage.claim.ClaimRepository;
import com.steelsnake.cardamage.claim.ClaimStatus;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
class CarDamageApplicationTests {

	@Container
	@ServiceConnection
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18-alpine")
			.withDatabaseName("car_damage_test");

	@Autowired
	private ClaimRepository claimRepository;

	@Autowired
	private ClaimImageRepository claimImageRepository;

	@Test
	void migrationsRunAndRepositoriesPersistCoreData() {
		Instant now = Instant.now();
		Claim newClaim = new Claim(null, "Toyota", "Camry", 2022, ClaimStatus.ANALYSIS_PENDING, now, now);

		Mono<PersistedData> persistedData = this.claimRepository.save(newClaim)
				.flatMap(savedClaim -> this.claimImageRepository.save(new ClaimImage(
						null,
						savedClaim.id(),
						"claims/test/damage.jpg",
						"damage.jpg",
						"image/jpeg",
						1024,
						now))
						.then(Mono.zip(
								this.claimRepository.findById(savedClaim.id()),
								this.claimImageRepository.findAllByClaimId(savedClaim.id()).single()))
						.map(values -> new PersistedData(values.getT1(), values.getT2())));

		StepVerifier.create(persistedData)
				.assertNext(data -> {
					assertThat(data.claim().id()).isNotNull();
					assertThat(data.claim().carBrand()).isEqualTo("Toyota");
					assertThat(data.claim().status()).isEqualTo(ClaimStatus.ANALYSIS_PENDING);
					assertThat(data.image().id()).isNotNull();
					assertThat(data.image().claimId()).isEqualTo(data.claim().id());
				})
				.expectComplete()
				.verify(Duration.ofSeconds(30));
	}

	private record PersistedData(Claim claim, ClaimImage image) {
	}
}
