package com.steelsnake.cardamage;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.imageio.ImageIO;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;

import com.steelsnake.cardamage.auth.AdminUserRepository;
import com.steelsnake.cardamage.claim.Claim;
import com.steelsnake.cardamage.claim.ClaimImage;
import com.steelsnake.cardamage.claim.ClaimImageRepository;
import com.steelsnake.cardamage.claim.ClaimRepository;
import com.steelsnake.cardamage.claim.ClaimService;
import com.steelsnake.cardamage.claim.ClaimStatus;
import com.steelsnake.cardamage.claim.ClaimStatusResponse;
import com.steelsnake.cardamage.claim.DamageAnalysisRequested;
import com.steelsnake.cardamage.claim.ImageStorage;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
class CarDamageApplicationTests {

	@Container
	@ServiceConnection
	static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18-alpine")
			.withDatabaseName("car_damage_test");
	@Container
	@ServiceConnection
	static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.1.1");
	private static final Path imageDirectory = Path.of(
			"build", "test-images", UUID.randomUUID().toString()).toAbsolutePath();
	private static final String adminPassword = "integration-test-password";
	private static final String adminPasswordHash =
			new org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder().encode(adminPassword);

	@Autowired
	private ClaimRepository claimRepository;

	@Autowired
	private ClaimImageRepository claimImageRepository;
	@Autowired
	private AdminUserRepository adminUserRepository;
	@Autowired
	private PasswordEncoder passwordEncoder;
	@Autowired
	private ApplicationContext applicationContext;
	@Autowired
	private KafkaTemplate<String, DamageAnalysisRequested> kafkaTemplate;
	@Autowired
	private KafkaListenerEndpointRegistry listenerRegistry;
	@MockitoSpyBean
	private ClaimService claimService;

	private WebTestClient webTestClient;

	@DynamicPropertySource
	static void imageStorageProperties(DynamicPropertyRegistry registry) {
		registry.add("app.image-storage.directory", imageDirectory::toString);
		registry.add("app.security.jwt.secret", () -> "integration-test-jwt-secret-at-least-32-bytes");
		registry.add("spring.flyway.placeholders.admin-email", () -> "admin@integration.test");
		registry.add("spring.flyway.placeholders.admin-password-hash", () -> adminPasswordHash);
		registry.add("app.kafka.analysis-retry-interval", () -> "10ms");
	}

	@BeforeEach
	void configureWebTestClient() {
		this.webTestClient = WebTestClient.bindToApplicationContext(this.applicationContext)
				.configureClient()
				.build();
	}

	@Test
	void repositoriesPersistClaimsAndImages() {
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
	void migrationPersistsAdminWithBcryptPassword() {
		StepVerifier.create(this.adminUserRepository.findByEmail("admin@integration.test"))
				.assertNext(admin -> {
					assertThat(admin.passwordHash()).startsWith("$2");
					assertThat(admin.passwordHash()).doesNotContain(adminPassword);
					assertThat(this.passwordEncoder.matches(adminPassword, admin.passwordHash())).isTrue();
				})
				.expectComplete()
				.verify(Duration.ofSeconds(10));
	}

	@Test
	void claimSubmissionPersistsImagesAndExposesOnlyPublicStatus() throws IOException {
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

		ClaimStatusResponse created = this.webTestClient.post()
				.uri("/api/claims")
				.contentType(MediaType.MULTIPART_FORM_DATA)
				.body(BodyInserters.fromMultipartData(body.build()))
				.exchange()
				.expectStatus().isAccepted()
				.expectHeader().valueMatches("Location", "/api/claims/.+/status")
				.expectBody(ClaimStatusResponse.class)
				.returnResult()
				.getResponseBody();

		assertThat(created).isNotNull();
		assertThat(created.status()).isEqualTo(ClaimStatus.ANALYSIS_PENDING);

		StepVerifier.create(Mono.zip(
				this.claimRepository.findById(created.id()),
				this.claimImageRepository.findAllByClaimId(created.id()).single()))
				.assertNext(values -> {
					assertThat(values.getT1().carBrand()).isEqualTo("Toyota");
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
				.jsonPath("$.carBrand").doesNotExist()
				.jsonPath("$.carModel").doesNotExist();
	}

	@Test
	void publishedAnalysisRequestTransitionsClaimToAnalyzing() throws Exception {
		UUID claimId = submitClaim();

		Claim analyzing = awaitStatus(claimId, ClaimStatus.ANALYZING);

		assertThat(analyzing.status()).isEqualTo(ClaimStatus.ANALYZING);
		assertThat(analyzing.updatedAt()).isAfterOrEqualTo(analyzing.createdAt());
	}

	@Test
	void redeliveredAnalysisRequestDoesNotRepeatProcessing() throws Exception {
		UUID claimId = submitClaim();
		Claim analyzing = awaitStatus(claimId, ClaimStatus.ANALYZING);

		var redelivered = this.kafkaTemplate.send(
						DamageAnalysisRequested.TOPIC,
						claimId.toString(),
						new DamageAnalysisRequested(DamageAnalysisRequested.VERSION, claimId))
				.get(10, TimeUnit.SECONDS);
		awaitCommittedOffset(
				new TopicPartition(
						redelivered.getRecordMetadata().topic(), redelivered.getRecordMetadata().partition()),
				redelivered.getRecordMetadata().offset() + 1);

		Claim afterRedelivery = this.claimRepository.findById(claimId).block(Duration.ofSeconds(10));
		assertThat(afterRedelivery).isNotNull();
		assertThat(afterRedelivery.status()).isEqualTo(ClaimStatus.ANALYZING);
		assertThat(afterRedelivery.updatedAt()).isEqualTo(analyzing.updatedAt());
	}

	@Test
	void malformedRecordDoesNotStallLaterAnalysisRequests() throws Exception {
		Map<String, Object> producerProperties = Map.of(
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
				ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
				ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		DefaultKafkaProducerFactory<String, String> producerFactory =
				new DefaultKafkaProducerFactory<>(producerProperties);
		TopicPartition malformedPartition;
		long offsetAfterMalformed;
		try {
			var malformed = new KafkaTemplate<>(producerFactory)
					.send(DamageAnalysisRequested.TOPIC, "malformed", "{not json")
					.get(10, TimeUnit.SECONDS);
			malformedPartition = new TopicPartition(
					malformed.getRecordMetadata().topic(), malformed.getRecordMetadata().partition());
			offsetAfterMalformed = malformed.getRecordMetadata().offset() + 1;
		}
		finally {
			producerFactory.destroy();
		}

		awaitCommittedOffset(malformedPartition, offsetAfterMalformed);
		MessageListenerContainer container = analysisListenerContainer();
		try {
			stopAndAwait(container);
			assertThat(committedOffset(malformedPartition)).isGreaterThanOrEqualTo(offsetAfterMalformed);

			container.start();
			assertThat(container.isRunning()).isTrue();
			assertThat(committedOffset(malformedPartition)).isGreaterThanOrEqualTo(offsetAfterMalformed);

			UUID claimId = submitClaim();

			assertThat(awaitStatus(claimId, ClaimStatus.ANALYZING).status()).isEqualTo(ClaimStatus.ANALYZING);
		}
		finally {
			if (!container.isRunning()) {
				container.start();
			}
		}
	}

	@Test
	void temporaryPersistenceFailureIsRetriedAndUltimatelyProcessed() throws Exception {
		Claim pending = savePendingClaim("Toyota", "Camry", 2022);
		doReturn(Mono.error(new TransientDataAccessResourceException("database is down")))
				.doCallRealMethod()
				.when(this.claimService).startAnalysis(pending.id());

		this.kafkaTemplate.send(
						DamageAnalysisRequested.TOPIC,
						pending.id().toString(),
						new DamageAnalysisRequested(DamageAnalysisRequested.VERSION, pending.id()))
				.get(10, TimeUnit.SECONDS);

		assertThat(awaitStatus(pending.id(), ClaimStatus.ANALYZING).status())
				.isEqualTo(ClaimStatus.ANALYZING);
		verify(this.claimService, times(2)).startAnalysis(pending.id());

		Claim followUp = savePendingClaim("Honda", "Civic", 2023);
		var followUpSent = this.kafkaTemplate.send(
						DamageAnalysisRequested.TOPIC,
						followUp.id().toString(),
						new DamageAnalysisRequested(DamageAnalysisRequested.VERSION, followUp.id()))
				.get(10, TimeUnit.SECONDS);

		assertThat(awaitStatus(followUp.id(), ClaimStatus.ANALYZING).status())
				.isEqualTo(ClaimStatus.ANALYZING);
		awaitCommittedOffset(
				new TopicPartition(
						followUpSent.getRecordMetadata().topic(), followUpSent.getRecordMetadata().partition()),
				followUpSent.getRecordMetadata().offset() + 1);
	}

	@Test
	void persistenceFailureKeepsRecordRedeliverableAcrossListenerRestart() throws Exception {
		Claim pending = savePendingClaim("Toyota", "Corolla", 2021);
		CountDownLatch firstAttemptFailed = new CountDownLatch(1);
		CountDownLatch oldAttemptParked = new CountDownLatch(1);
		CountDownLatch postRestartDelivery = new CountDownLatch(1);
		AtomicBoolean persistenceRecovered = new AtomicBoolean();
		AtomicInteger processingAttempts = new AtomicInteger();
		Sinks.One<Void> parkedOldAttempt = Sinks.one();
		doAnswer(invocation -> {
			int attempt = processingAttempts.incrementAndGet();
			if (attempt == 1) {
				firstAttemptFailed.countDown();
				return Mono.error(new TransientDataAccessResourceException("database is down"));
			}
			if (attempt == 2) {
				oldAttemptParked.countDown();
				return parkedOldAttempt.asMono();
			}
			if (persistenceRecovered.get()) {
				postRestartDelivery.countDown();
				return invocation.callRealMethod();
			}
			return Mono.error(new IllegalStateException("processing resumed before persistence recovery"));
		}).when(this.claimService).startAnalysis(pending.id());

		var sent = this.kafkaTemplate.send(
						DamageAnalysisRequested.TOPIC,
						pending.id().toString(),
						new DamageAnalysisRequested(DamageAnalysisRequested.VERSION, pending.id()))
				.get(10, TimeUnit.SECONDS);
		TopicPartition partition = new TopicPartition(
				sent.getRecordMetadata().topic(), sent.getRecordMetadata().partition());
		assertThat(firstAttemptFailed.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(oldAttemptParked.await(30, TimeUnit.SECONDS)).isTrue();

		MessageListenerContainer container = analysisListenerContainer();
		try {
			stopAndAwait(container);

			Claim beforeRestart = this.claimRepository.findById(pending.id()).block(Duration.ofSeconds(10));
			assertThat(beforeRestart).isNotNull();
			assertThat(beforeRestart.status()).isEqualTo(ClaimStatus.ANALYSIS_PENDING);
			assertThat(committedOffset(partition)).isLessThanOrEqualTo(sent.getRecordMetadata().offset());

			persistenceRecovered.set(true);
			container.start();
			assertThat(container.isRunning()).isTrue();

			assertThat(postRestartDelivery.await(30, TimeUnit.SECONDS)).isTrue();
			assertThat(awaitStatus(pending.id(), ClaimStatus.ANALYZING).status())
					.isEqualTo(ClaimStatus.ANALYZING);
			awaitCommittedOffset(partition, sent.getRecordMetadata().offset() + 1);
		}
		finally {
			parkedOldAttempt.tryEmitEmpty();
			if (!container.isRunning()) {
				container.start();
			}
		}
	}

	@Test
	void invalidLaterImageLeavesDatabaseAndStorageUnchanged() throws IOException {
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
		assertStagingIsEmpty();
	}

	@Test
	void multipartPartLimitReturnsPayloadTooLarge() throws IOException {
		MultipartBodyBuilder body = vehicleParts();
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
	void multipartSizeLimitReturnsPayloadTooLarge() {
		MultipartBodyBuilder body = vehicleParts();
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

		MultipartBodyBuilder formFieldImage = vehicleParts();
		formFieldImage.part("images", "not-a-file");
		postMultipart(formFieldImage).expectStatus().isBadRequest();

		MultipartBodyBuilder unexpected = vehicleParts();
		unexpected.part("notes", "unexpected");
		unexpected.part("images", imageResource("damage.png", createPng())).contentType(MediaType.IMAGE_PNG);
		postMultipart(unexpected).expectStatus().isBadRequest();

		MultipartBodyBuilder duplicateBrand = vehicleParts();
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

	private UUID submitClaim() throws IOException {
		MultipartBodyBuilder body = vehicleParts();
		body.part("images", imageResource("damage.png", createPng())).contentType(MediaType.IMAGE_PNG);

		ClaimStatusResponse accepted = postMultipart(body)
				.expectStatus().isAccepted()
				.expectBody(ClaimStatusResponse.class)
				.returnResult()
				.getResponseBody();

		assertThat(accepted).isNotNull();
		assertThat(accepted.status()).isEqualTo(ClaimStatus.ANALYSIS_PENDING);
		return accepted.id();
	}

	private Claim awaitStatus(UUID claimId, ClaimStatus expected) throws InterruptedException {
		for (int attempt = 0; attempt < 60; attempt++) {
			Claim claim = this.claimRepository.findById(claimId).block(Duration.ofSeconds(10));
			if (claim != null && claim.status() == expected) {
				return claim;
			}
			Thread.sleep(500);
		}
		throw new AssertionError("Claim " + claimId + " did not reach " + expected);
	}

	private Claim savePendingClaim(String brand, String model, int year) {
		Instant now = Instant.now();
		Claim pending = this.claimRepository.save(new Claim(
				null, brand, model, year, ClaimStatus.ANALYSIS_PENDING, now, now))
				.block(Duration.ofSeconds(10));
		assertThat(pending).isNotNull();
		return pending;
	}

	private MessageListenerContainer analysisListenerContainer() {
		MessageListenerContainer container = this.listenerRegistry.getListenerContainers().stream()
				.findFirst()
				.orElseThrow(() -> new AssertionError("Analysis listener container is missing"));
		assertThat(container.isRunning()).isTrue();
		return container;
	}

	private static void stopAndAwait(MessageListenerContainer container) throws InterruptedException {
		CountDownLatch stopped = new CountDownLatch(1);
		container.stop(stopped::countDown);
		assertThat(stopped.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(container.isRunning()).isFalse();
	}

	private void awaitCommittedOffset(TopicPartition partition, long expectedOffset) throws Exception {
		for (int attempt = 0; attempt < 300; attempt++) {
			if (committedOffset(partition) >= expectedOffset) {
				return;
			}
			Thread.sleep(100);
		}
		throw new AssertionError("Consumer did not commit offset " + expectedOffset + " for " + partition);
	}

	private long committedOffset(TopicPartition partition) throws Exception {
		Map<String, Object> adminProperties = Map.of(
				AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
		try (Admin admin = Admin.create(adminProperties)) {
			OffsetAndMetadata committed = admin.listConsumerGroupOffsets("car-damage-analysis")
					.partitionsToOffsetAndMetadata()
					.get(10, TimeUnit.SECONDS)
					.get(partition);
			return committed == null ? -1 : committed.offset();
		}
	}

	private static MultipartBodyBuilder vehicleParts() {
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

	private static void assertStagingIsEmpty() throws IOException {
		Path staging = imageDirectory.resolve(".staging");
		if (Files.exists(staging)) {
			try (var paths = Files.list(staging)) {
				assertThat(paths).isEmpty();
			}
		}
	}

	private record PersistedData(Claim claim, ClaimImage image) {
	}
}
