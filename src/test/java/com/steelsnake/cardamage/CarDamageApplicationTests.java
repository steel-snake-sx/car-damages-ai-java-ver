package com.steelsnake.cardamage;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import javax.imageio.ImageIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;

import com.steelsnake.cardamage.claim.Claim;
import com.steelsnake.cardamage.claim.ClaimCreatedResponse;
import com.steelsnake.cardamage.claim.ClaimImage;
import com.steelsnake.cardamage.claim.ClaimImageRepository;
import com.steelsnake.cardamage.claim.ClaimRepository;
import com.steelsnake.cardamage.claim.ClaimStatus;
import com.steelsnake.cardamage.claim.ImageStorage;
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
	private static final Path imageDirectory = Path.of(
			"build", "test-images", UUID.randomUUID().toString()).toAbsolutePath();

	@Autowired
	private ClaimRepository claimRepository;

	@Autowired
	private ClaimImageRepository claimImageRepository;
	@Autowired
	private ApplicationContext applicationContext;

	private WebTestClient webTestClient;

	@DynamicPropertySource
	static void imageStorageProperties(DynamicPropertyRegistry registry) {
		registry.add("app.image-storage.directory", imageDirectory::toString);
	}

	@BeforeEach
	void configureWebTestClient() {
		this.webTestClient = WebTestClient.bindToApplicationContext(this.applicationContext)
				.configureClient()
				.build();
	}

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

	@Test
	void claimsApiPersistsValidMultipartRequestAndExposesOnlyStatusPublicly() throws IOException {
		byte[] png = createPng();
		MultipartBodyBuilder body = new MultipartBodyBuilder();
		body.part("carBrand", "Toyota");
		body.part("carModel", "Camry");
		body.part("carYear", "2022");
		body.part("images", new ByteArrayResource(png) {
			@Override
			public String getFilename() {
				return "damage.png";
			}
		}).contentType(MediaType.IMAGE_PNG);

		ClaimCreatedResponse created = this.webTestClient.post()
				.uri("/api/claims")
				.contentType(MediaType.MULTIPART_FORM_DATA)
				.body(BodyInserters.fromMultipartData(body.build()))
				.exchange()
				.expectStatus().isCreated()
				.expectHeader().valueMatches("Location", "/api/claims/.+/status")
				.expectBody(ClaimCreatedResponse.class)
				.returnResult()
				.getResponseBody();

		assertThat(created).isNotNull();
		assertThat(created.status()).isEqualTo(ClaimStatus.ANALYSIS_PENDING);

		StepVerifier.create(Mono.zip(
				this.claimRepository.findById(created.id()),
				this.claimImageRepository.findAllByClaimId(created.id()).single()))
				.assertNext(values -> {
					assertThat(values.getT1().carBrand()).isEqualTo("Toyota");
					assertThat(values.getT1().status()).isEqualTo(ClaimStatus.ANALYSIS_PENDING);
					assertThat(values.getT2().originalFilename()).isEqualTo("damage.png");
					assertThat(Files.exists(imageDirectory.resolve(values.getT2().storagePath()))).isTrue();
				})
				.expectComplete()
				.verify(Duration.ofSeconds(10));

		this.webTestClient.get()
				.uri("/api/claims/{id}/status", created.id())
				.exchange()
				.expectStatus().isOk()
				.expectBody()
				.jsonPath("$.id").isEqualTo(created.id().toString())
				.jsonPath("$.status").isEqualTo("ANALYSIS_PENDING")
				.jsonPath("$.carBrand").doesNotExist()
				.jsonPath("$.carModel").doesNotExist();
	}

	@Test
	void invalidLaterImageRollsBackDatabaseAndRemovesStoredFiles() throws IOException {
		long claimsBefore = this.claimRepository.count().block(Duration.ofSeconds(10));
		long imagesBefore = this.claimImageRepository.count().block(Duration.ofSeconds(10));
		long directoriesBefore = countImageDirectories();

		MultipartBodyBuilder body = new MultipartBodyBuilder();
		body.part("carBrand", "Toyota");
		body.part("carModel", "Camry");
		body.part("carYear", "2022");
		body.part("images", imageResource("damage.png", createPng())).contentType(MediaType.IMAGE_PNG);
		body.part("images", imageResource(
				"broken.jpg",
				new byte[] {(byte) 0xFF, (byte) 0xD8, (byte) 0xFF, 0x01}))
				.contentType(MediaType.IMAGE_JPEG);

		this.webTestClient.post()
				.uri("/api/claims")
				.contentType(MediaType.MULTIPART_FORM_DATA)
				.body(BodyInserters.fromMultipartData(body.build()))
				.exchange()
				.expectStatus().isBadRequest();

		StepVerifier.create(Mono.zip(this.claimRepository.count(), this.claimImageRepository.count()))
				.assertNext(counts -> {
					assertThat(counts.getT1()).isEqualTo(claimsBefore);
					assertThat(counts.getT2()).isEqualTo(imagesBefore);
				})
				.expectComplete()
				.verify(Duration.ofSeconds(10));
		assertThat(countImageDirectories()).isEqualTo(directoriesBefore);
	}

	@Test
	void moreThanSixMultipartPartsIsRejectedByTheActiveHttpReader() throws IOException {
		MultipartBodyBuilder body = validClaimBody();
		byte[] png = createPng();
		for (int image = 0; image < 4; image++) {
			body.part("images", imageResource("damage-" + image + ".png", png))
					.contentType(MediaType.IMAGE_PNG);
		}

		this.webTestClient.post()
				.uri("/api/claims")
				.contentType(MediaType.MULTIPART_FORM_DATA)
				.body(BodyInserters.fromMultipartData(body.build()))
				.exchange()
				.expectStatus().value(status -> assertThat(status).isEqualTo(413))
				.expectBody()
				.jsonPath("$.status").isEqualTo(413);
	}

	@Test
	void oversizedImagePartIsRejectedByTheActiveHttpReader() {
		MultipartBodyBuilder body = validClaimBody();
		body.part("images", imageResource(
				"oversized.png", new byte[(int) ImageStorage.MAX_IMAGE_SIZE_BYTES + 1]))
				.contentType(MediaType.IMAGE_PNG);

		this.webTestClient.post()
				.uri("/api/claims")
				.contentType(MediaType.MULTIPART_FORM_DATA)
				.body(BodyInserters.fromMultipartData(body.build()))
				.exchange()
				.expectStatus().value(status -> assertThat(status).isEqualTo(413))
				.expectBody()
				.jsonPath("$.status").isEqualTo(413);
	}

	@Test
	void malformedMultipartStructureAndPartTypesReturnBadRequest() throws IOException {
		this.webTestClient.post()
				.uri("/api/claims")
				.contentType(MediaType.MULTIPART_FORM_DATA)
				.bodyValue("missing boundary")
				.exchange()
				.expectStatus().isBadRequest();

		MultipartBodyBuilder fileBrand = new MultipartBodyBuilder();
		fileBrand.part("carBrand", imageResource("brand.png", createPng())).contentType(MediaType.IMAGE_PNG);
		fileBrand.part("carModel", "Camry");
		fileBrand.part("carYear", "2022");
		fileBrand.part("images", imageResource("damage.png", createPng())).contentType(MediaType.IMAGE_PNG);
		postMultipart(fileBrand).expectStatus().isBadRequest();

		MultipartBodyBuilder formFieldImage = validClaimBody();
		formFieldImage.part("images", "not-a-file");
		postMultipart(formFieldImage).expectStatus().isBadRequest();

		MultipartBodyBuilder unexpected = validClaimBody();
		unexpected.part("notes", "unexpected");
		unexpected.part("images", imageResource("damage.png", createPng())).contentType(MediaType.IMAGE_PNG);
		postMultipart(unexpected).expectStatus().isBadRequest();

		MultipartBodyBuilder duplicateBrand = validClaimBody();
		duplicateBrand.part("carBrand", "Honda");
		duplicateBrand.part("images", imageResource("damage.png", createPng())).contentType(MediaType.IMAGE_PNG);
		postMultipart(duplicateBrand).expectStatus().isBadRequest();
	}

	@Test
	void controlCharacterInVehicleFieldReturnsBadRequest() throws IOException {
		MultipartBodyBuilder body = new MultipartBodyBuilder();
		body.part("carBrand", "Toy\0ota");
		body.part("carModel", "Camry");
		body.part("carYear", "2022");
		body.part("images", imageResource("damage.png", createPng())).contentType(MediaType.IMAGE_PNG);

		postMultipart(body)
				.expectStatus().isBadRequest()
				.expectBody()
				.jsonPath("$.message").isEqualTo("carBrand must not contain control characters");

		MultipartBodyBuilder leadingControl = new MultipartBodyBuilder();
		leadingControl.part("carBrand", "\nToyota");
		leadingControl.part("carModel", "Camry");
		leadingControl.part("carYear", "2022");
		leadingControl.part("images", imageResource("damage.png", createPng()))
				.contentType(MediaType.IMAGE_PNG);
		postMultipart(leadingControl).expectStatus().isBadRequest();
	}

	private WebTestClient.ResponseSpec postMultipart(MultipartBodyBuilder body) {
		return this.webTestClient.post()
				.uri("/api/claims")
				.contentType(MediaType.MULTIPART_FORM_DATA)
				.body(BodyInserters.fromMultipartData(body.build()))
				.exchange();
	}

	private static MultipartBodyBuilder validClaimBody() {
		MultipartBodyBuilder body = new MultipartBodyBuilder();
		body.part("carBrand", "Toyota");
		body.part("carModel", "Camry");
		body.part("carYear", "2022");
		return body;
	}

	private static ByteArrayResource imageResource(String filename, byte[] content) {
		return new ByteArrayResource(content) {
			@Override
			public String getFilename() {
				return filename;
			}
		};
	}

	private static byte[] createPng() throws IOException {
		BufferedImage image = new BufferedImage(2, 2, BufferedImage.TYPE_INT_RGB);
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		if (!ImageIO.write(image, "png", output)) {
			throw new IllegalStateException("PNG writer is unavailable");
		}
		return output.toByteArray();
	}

	private static long countImageDirectories() throws IOException {
		if (!Files.exists(imageDirectory)) {
			return 0;
		}
		try (var paths = Files.list(imageDirectory)) {
			return paths.filter(Files::isDirectory)
					.filter(path -> !path.getFileName().toString().equals(".staging"))
					.count();
		}
	}

	private record PersistedData(Claim claim, ClaimImage image) {
	}
}
