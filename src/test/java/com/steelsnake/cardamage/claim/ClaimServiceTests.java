package com.steelsnake.cardamage.claim;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.imageio.ImageIO;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.reactive.AbstractReactiveTransactionManager;
import org.springframework.transaction.reactive.GenericReactiveTransaction;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.reactive.TransactionalOperator;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ClaimServiceTests {

	@TempDir
	Path temporaryDirectory;

	private ClaimRepository claimRepository;
	private ClaimImageRepository imageRepository;
	private TestTransactionManager transactionManager;
	private KafkaTemplate<String, DamageAnalysisRequested> kafkaTemplate;
	private ClaimService service;

	@BeforeEach
	@SuppressWarnings("unchecked")
	void configureService() {
		this.claimRepository = mock(ClaimRepository.class);
		this.imageRepository = mock(ClaimImageRepository.class);
		this.transactionManager = new TestTransactionManager();
		this.kafkaTemplate = mock(KafkaTemplate.class);
		this.service = new ClaimService(
				this.claimRepository,
				this.imageRepository,
				new ImageStorage(this.temporaryDirectory.toString()),
				TransactionalOperator.create(this.transactionManager),
				this.kafkaTemplate);
	}

	@Test
	void persistsValidClaimAndImageMetadataAsPending() throws IOException {
		UUID claimId = configureSuccessfulPersistence();

		StepVerifier.create(this.service.createClaim(
				" Toyota ", " Camry ", 2022, List.of(filePart(createPng()))))
				.assertNext(response -> {
					assertThat(response.id()).isEqualTo(claimId);
					assertThat(response.status()).isEqualTo(ClaimStatus.ANALYSIS_PENDING);
				})
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		assertThat(this.transactionManager.commits).isEqualTo(1);
		assertThat(this.transactionManager.rollbacks).isZero();
		assertThat(this.temporaryDirectory.resolve(claimId.toString())).isDirectory();
		assertStagingIsEmpty();
		verify(this.imageRepository).save(any(ClaimImage.class));
	}

	@Test
	void publishesOneClaimReferenceEventKeyedByClaimId() throws IOException {
		UUID claimId = configureSuccessfulPersistence();

		StepVerifier.create(this.service.createClaim(
				"Toyota", "Camry", 2022, List.of(filePart(createPng()))))
				.expectNextCount(1)
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		verify(this.kafkaTemplate, times(1)).send(
				eq(DamageAnalysisRequested.TOPIC),
				eq(claimId.toString()),
				eq(new DamageAnalysisRequested(1, claimId)));
	}

	@Test
	void publishesOnlyAfterTheClaimTransactionCommits() throws IOException {
		UUID claimId = configureClaimSave();
		when(this.imageRepository.save(any(ClaimImage.class))).thenAnswer(invocation -> {
			ClaimImage image = invocation.getArgument(0);
			return Mono.just(new ClaimImage(
					UUID.randomUUID(), image.claimId(), image.storagePath(), image.originalFilename(),
					image.contentType(), image.sizeBytes(), image.createdAt()));
		});
		CompletableFuture<SendResult<String, DamageAnalysisRequested>> pendingSend = new CompletableFuture<>();
		CountDownLatch sendStarted = new CountDownLatch(1);
		int[] commitsWhenSendStarted = new int[1];
		when(this.kafkaTemplate.send(anyString(), anyString(), any(DamageAnalysisRequested.class)))
				.thenAnswer(invocation -> {
					commitsWhenSendStarted[0] = this.transactionManager.commits;
					sendStarted.countDown();
					return pendingSend;
				});

		StepVerifier.create(this.service.createClaim(
				"Toyota", "Camry", 2022, List.of(filePart(createPng()))))
				.then(() -> {
					assertThat(await(sendStarted)).isTrue();
					pendingSend.complete(sendResult(claimId));
				})
				.expectNextCount(1)
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		assertThat(commitsWhenSendStarted[0]).isEqualTo(1);
		assertThat(this.transactionManager.rollbacks).isZero();
	}

	@Test
	void publishFailureReportsUnavailableDispatchAndKeepsCommittedFiles() throws IOException {
		UUID claimId = configureSuccessfulPersistence();
		when(this.kafkaTemplate.send(anyString(), anyString(), any(DamageAnalysisRequested.class)))
				.thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

		StepVerifier.create(this.service.createClaim(
				"Toyota", "Camry", 2022, List.of(filePart(createPng()))))
				.expectErrorSatisfies(error -> assertThat(error)
						.isInstanceOf(ClaimApiException.class)
						.satisfies(failure -> assertThat(((ClaimApiException) failure).status())
								.isEqualTo(HttpStatus.SERVICE_UNAVAILABLE))
						.hasMessageContaining(claimId.toString()))
				.verify(Duration.ofSeconds(5));

		assertThat(this.transactionManager.commits).isEqualTo(1);
		assertThat(this.transactionManager.rollbacks).isZero();
		assertThat(this.temporaryDirectory.resolve(claimId.toString())).isDirectory();
		assertStagingIsEmpty();
	}

	@Test
	void startAnalysisTransitionsEligibleClaimAndIgnoresRedelivery() {
		UUID claimId = UUID.randomUUID();
		when(this.claimRepository.startAnalysis(eq(claimId), any(Instant.class)))
				.thenReturn(Mono.just(1L))
				.thenReturn(Mono.just(0L));

		StepVerifier.create(this.service.startAnalysis(claimId))
				.expectComplete()
				.verify(Duration.ofSeconds(1));
		StepVerifier.create(this.service.startAnalysis(claimId))
				.expectComplete()
				.verify(Duration.ofSeconds(1));

		verify(this.claimRepository, times(2)).startAnalysis(eq(claimId), any(Instant.class));
		verifyNoInteractions(this.imageRepository);
	}

	@Test
	void persistenceFailureRollsBackAndRemovesFinalizedFiles() throws IOException {
		UUID claimId = configureClaimSave();
		when(this.imageRepository.save(any(ClaimImage.class)))
				.thenReturn(Mono.error(new IllegalStateException("database error")));

		StepVerifier.create(this.service.createClaim(
				"Toyota", "Camry", 2022, List.of(filePart(createPng()))))
				.expectErrorMessage("database error")
				.verify(Duration.ofSeconds(5));

		assertThat(this.transactionManager.rollbacks).isEqualTo(1);
		assertThat(this.temporaryDirectory.resolve(claimId.toString())).doesNotExist();
		assertStagingIsEmpty();
		verifyNoInteractions(this.kafkaTemplate);
	}

	@Test
	void failedCommitRemovesFinalizedFiles() throws IOException {
		UUID claimId = configureSuccessfulPersistence();
		this.transactionManager.commitFailure = new IllegalStateException("commit error");

		StepVerifier.create(this.service.createClaim(
				"Toyota", "Camry", 2022, List.of(filePart(createPng()))))
				.expectErrorSatisfies(error -> assertThat(error).hasMessageContaining("commit error"))
				.verify(Duration.ofSeconds(5));

		assertThat(this.temporaryDirectory.resolve(claimId.toString())).doesNotExist();
		assertStagingIsEmpty();
	}

	@Test
	void unknownCommitOutcomeKeepsFiles() throws IOException {
		UUID claimId = configureSuccessfulPersistence();
		this.transactionManager.commitFailure = new TransactionSystemException("commit outcome unknown");

		StepVerifier.create(this.service.createClaim(
				"Toyota", "Camry", 2022, List.of(filePart(createPng()))))
				.expectErrorSatisfies(error -> assertThat(error).hasMessageContaining("commit outcome unknown"))
				.verify(Duration.ofSeconds(5));

		assertThat(this.temporaryDirectory.resolve(claimId.toString())).isDirectory();
		assertStagingIsEmpty();
	}

	@Test
	void cancellationBeforeTransactionBeginsRemovesStagedFiles() throws Exception {
		configureSuccessfulPersistence();
		this.transactionManager.beginNeverCompletes = true;

		StepVerifier.create(this.service.createClaim(
				"Toyota", "Camry", 2022, List.of(filePart(createPng()))))
				.then(() -> assertThat(await(this.transactionManager.beginStarted)).isTrue())
				.thenCancel()
				.verify(Duration.ofSeconds(5));

		assertStagingIsEmptyEventually();
		assertThat(countClaimDirectories()).isZero();
	}

	@Test
	void cancellationDuringSuccessfulCommitNeverDeletesCommittedFiles() throws Exception {
		UUID claimId = configureSuccessfulPersistence();
		this.transactionManager.delayCommit = true;

		StepVerifier.create(this.service.createClaim(
				"Toyota", "Camry", 2022, List.of(filePart(createPng()))))
				.then(() -> assertThat(await(this.transactionManager.commitStarted)).isTrue())
				.thenCancel()
				.verify(Duration.ofSeconds(10));

		this.transactionManager.completeCommit();
		assertThat(this.transactionManager.commitFinished.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(this.transactionManager.commitCompleted).isTrue();
		assertThat(this.transactionManager.rollbacks).isZero();
		assertThat(this.temporaryDirectory.resolve(claimId.toString())).isDirectory();
		assertStagingIsEmpty();
		verify(this.imageRepository).save(any(ClaimImage.class));
	}

	@Test
	void statusContainsOnlyPublicProcessingFields() {
		UUID claimId = UUID.randomUUID();
		Instant now = Instant.now();
		when(this.claimRepository.findById(claimId)).thenReturn(Mono.just(new Claim(
				claimId, "Toyota", "Camry", 2022, ClaimStatus.ANALYSIS_PENDING, now, now)));

		StepVerifier.create(this.service.getStatus(claimId))
				.assertNext(status -> assertThat(status)
						.isEqualTo(new ClaimStatusResponse(claimId, ClaimStatus.ANALYSIS_PENDING)))
				.expectComplete()
				.verify(Duration.ofSeconds(1));
	}

	@Test
	void rejectsClaimWithoutImagesBeforePersistence() {
		StepVerifier.create(this.service.createClaim("Toyota", "Camry", 2022, List.of()))
				.expectErrorSatisfies(error -> assertThat(error)
						.isInstanceOf(ClaimApiException.class)
						.hasMessage("Between 1 and 3 images are required"))
				.verify(Duration.ofSeconds(1));

		verifyNoInteractions(this.claimRepository, this.imageRepository);
	}

	@Test
	void rejectsMoreThanThreeImagesBeforePersistence() {
		var images = List.of(
				mock(FilePart.class), mock(FilePart.class), mock(FilePart.class), mock(FilePart.class));

		StepVerifier.create(this.service.createClaim("Toyota", "Camry", 2022, images))
				.expectError(ClaimApiException.class)
				.verify(Duration.ofSeconds(1));

		verifyNoInteractions(this.claimRepository, this.imageRepository);
	}

	private UUID configureSuccessfulPersistence() {
		UUID claimId = configureClaimSave();
		when(this.imageRepository.save(any(ClaimImage.class))).thenAnswer(invocation -> {
			ClaimImage image = invocation.getArgument(0);
			return Mono.just(new ClaimImage(
					UUID.randomUUID(), image.claimId(), image.storagePath(), image.originalFilename(),
					image.contentType(), image.sizeBytes(), image.createdAt()));
		});
		when(this.kafkaTemplate.send(anyString(), anyString(), any(DamageAnalysisRequested.class)))
				.thenReturn(CompletableFuture.completedFuture(sendResult(claimId)));
		return claimId;
	}

	private static SendResult<String, DamageAnalysisRequested> sendResult(UUID claimId) {
		TopicPartition partition = new TopicPartition(DamageAnalysisRequested.TOPIC, 0);
		return new SendResult<>(
				new ProducerRecord<>(
						DamageAnalysisRequested.TOPIC,
						claimId.toString(),
						DamageAnalysisRequested.of(claimId)),
				new RecordMetadata(partition, 0L, 0, 0L, 0, 0));
	}

	private UUID configureClaimSave() {
		UUID claimId = UUID.randomUUID();
		when(this.claimRepository.save(any(Claim.class))).thenAnswer(invocation -> {
			Claim claim = invocation.getArgument(0);
			return Mono.just(new Claim(
					claimId, claim.carBrand(), claim.carModel(), claim.carYear(), claim.status(),
					claim.createdAt(), claim.updatedAt()));
		});
		return claimId;
	}

	private void assertStagingIsEmptyEventually() throws Exception {
		for (int attempt = 0; attempt < 50; attempt++) {
			if (isStagingEmpty()) {
				return;
			}
			Thread.sleep(20);
		}
		assertStagingIsEmpty();
	}

	private void assertStagingIsEmpty() throws IOException {
		assertThat(isStagingEmpty()).isTrue();
	}

	private boolean isStagingEmpty() throws IOException {
		Path stagingRoot = this.temporaryDirectory.resolve(".staging");
		if (!Files.exists(stagingRoot)) {
			return true;
		}
		try (var paths = Files.list(stagingRoot)) {
			return paths.findAny().isEmpty();
		}
	}

	private long countClaimDirectories() throws IOException {
		try (var paths = Files.list(this.temporaryDirectory)) {
			return paths.filter(Files::isDirectory)
					.filter(path -> !path.getFileName().toString().equals(".staging"))
					.count();
		}
	}

	private static FilePart filePart(byte[] content) {
		FilePart filePart = mock(FilePart.class);
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.IMAGE_PNG);
		when(filePart.filename()).thenReturn("damage.png");
		when(filePart.headers()).thenReturn(headers);
		when(filePart.transferTo(any(Path.class))).thenAnswer(invocation -> Mono.fromRunnable(() -> {
			try {
				Files.write(invocation.getArgument(0), content);
			}
			catch (IOException exception) {
				throw new UncheckedIOException(exception);
			}
		}));
		return filePart;
	}

	private static byte[] createPng() throws IOException {
		BufferedImage image = new BufferedImage(2, 2, BufferedImage.TYPE_INT_RGB);
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		if (!ImageIO.write(image, "png", output)) {
			throw new IllegalStateException("PNG writer is unavailable");
		}
		return output.toByteArray();
	}

	private static boolean await(CountDownLatch latch) {
		try {
			return latch.await(5, TimeUnit.SECONDS);
		}
		catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			throw new AssertionError("Interrupted while waiting for transaction", exception);
		}
	}

	@SuppressWarnings("serial")
	private static final class TestTransactionManager extends AbstractReactiveTransactionManager {

		private final CountDownLatch commitStarted = new CountDownLatch(1);
		private final CountDownLatch commitFinished = new CountDownLatch(1);
		private final CountDownLatch beginStarted = new CountDownLatch(1);
		private final Sinks.Empty<Void> commitGate = Sinks.empty();
		private int commits;
		private int rollbacks;
		private boolean beginNeverCompletes;
		private boolean delayCommit;
		private boolean commitCompleted;
		private RuntimeException commitFailure;

		@Override
		protected Object doGetTransaction(TransactionSynchronizationManager manager) throws TransactionException {
			return new Object();
		}

		@Override
		protected Mono<Void> doBegin(
				TransactionSynchronizationManager manager,
				Object transaction,
				TransactionDefinition definition) throws TransactionException {
			this.beginStarted.countDown();
			return this.beginNeverCompletes ? Mono.never() : Mono.empty();
		}

		@Override
		protected Mono<Void> doCommit(
				TransactionSynchronizationManager manager,
				GenericReactiveTransaction status) throws TransactionException {
			this.commits++;
			this.commitStarted.countDown();
			if (this.commitFailure != null) {
				return Mono.error(this.commitFailure);
			}
			return this.delayCommit
					? this.commitGate.asMono().doOnSuccess(unused -> markCommitCompleted())
					: Mono.fromRunnable(this::markCommitCompleted);
		}

		@Override
		protected Mono<Void> doRollback(
				TransactionSynchronizationManager manager,
				GenericReactiveTransaction status) throws TransactionException {
			this.rollbacks++;
			return Mono.empty();
		}

		void completeCommit() {
			assertThat(this.commitGate.tryEmitEmpty()).isEqualTo(Sinks.EmitResult.OK);
		}

		private void markCommitCompleted() {
			this.commitCompleted = true;
			this.commitFinished.countDown();
		}
	}
}
