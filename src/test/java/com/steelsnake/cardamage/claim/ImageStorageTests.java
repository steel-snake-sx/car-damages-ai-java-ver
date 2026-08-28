package com.steelsnake.cardamage.claim;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.zip.CRC32;

import javax.imageio.ImageIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.transaction.reactive.TransactionSynchronization;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ImageStorageTests {

	@TempDir
	Path temporaryDirectory;

	@Test
	void stagesAndMovesValidPngAndJpegUnderSafePaths() throws IOException {
		byte[] png = createImage("png");
		byte[] jpeg = createImage("jpg");
		ImageStorage storage = new ImageStorage(this.temporaryDirectory.toString());
		ImageStorage.ImageBatch batch = storage.createBatch();
		UUID claimId = UUID.randomUUID();

		StepVerifier.create(storage.stage(batch, List.of(
				filePart("../../damage.png", MediaType.IMAGE_PNG, png),
				filePart("damage.jpg", MediaType.IMAGE_JPEG, jpeg)))
				.then(Mono.defer(() -> {
					assertThat(batch.beginTransaction()).isTrue();
					return storage.moveToClaim(batch, claimId);
				})))
				.assertNext(images -> {
					assertThat(images).hasSize(2);
					assertThat(images.get(0).storagePath()).startsWith(claimId + "/").endsWith(".png");
					assertThat(images.get(0).originalFilename()).isEqualTo("damage.png");
					assertThat(images.get(1).storagePath()).startsWith(claimId + "/").endsWith(".jpg");
					assertThat(this.temporaryDirectory.resolve(images.get(0).storagePath())).hasBinaryContent(png);
					assertThat(this.temporaryDirectory.resolve(images.get(1).storagePath())).hasBinaryContent(jpeg);
				})
				.expectComplete()
				.verify(Duration.ofSeconds(5));
	}

	@Test
	void transactionCleanupWaitsForRegisteredMove() throws IOException {
		ImageStorage storage = new ImageStorage(this.temporaryDirectory.toString());
		ImageStorage.ImageBatch batch = storage.createBatch();
		UUID claimId = UUID.randomUUID();

		StepVerifier.create(storage.stage(batch, List.of(filePart(
				"damage.png", MediaType.IMAGE_PNG, createImage("png")))))
				.expectComplete()
				.verify(Duration.ofSeconds(5));
		assertThat(batch.beginTransaction()).isTrue();
		Mono<List<ImageStorage.StoredImage>> move = storage.moveToClaim(batch, claimId);

		StepVerifier.create(storage.completeTransaction(
				batch, TransactionSynchronization.STATUS_ROLLED_BACK))
				.expectComplete()
				.verify(Duration.ofSeconds(5));
		StepVerifier.create(move)
				.assertNext(images -> assertThat(images).hasSize(1))
				.expectComplete()
				.verify(Duration.ofSeconds(1));

		assertThat(stagingDirectory(batch)).doesNotExist();
		assertThat(this.temporaryDirectory.resolve(claimId.toString())).doesNotExist();
	}

	@Test
	void transactionCleanupPreventsLateMoveRegistration() throws IOException {
		ImageStorage storage = new ImageStorage(this.temporaryDirectory.toString());
		ImageStorage.ImageBatch batch = storage.createBatch();
		UUID claimId = UUID.randomUUID();

		StepVerifier.create(storage.stage(batch, List.of(filePart(
				"damage.png", MediaType.IMAGE_PNG, createImage("png")))))
				.expectComplete()
				.verify(Duration.ofSeconds(5));
		assertThat(batch.beginTransaction()).isTrue();
		Mono<Void> cleanup = storage.completeTransaction(
				batch, TransactionSynchronization.STATUS_ROLLED_BACK);

		StepVerifier.create(storage.moveToClaim(batch, claimId))
				.expectErrorSatisfies(error -> assertThat(error)
						.isInstanceOf(IllegalStateException.class)
						.hasMessage("Image cleanup was already claimed"))
				.verify(Duration.ofSeconds(1));
		StepVerifier.create(cleanup)
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		assertThat(stagingDirectory(batch)).doesNotExist();
		assertThat(this.temporaryDirectory.resolve(claimId.toString())).doesNotExist();
	}

	@Test
	void rejectsTruncatedJpegAndRemovesStagingDirectory() throws IOException {
		byte[] jpeg = createImage("jpg");
		assertMalformedImage(
				"broken.jpg",
				MediaType.IMAGE_JPEG,
				Arrays.copyOf(jpeg, jpeg.length - 2));
	}

	@Test
	void rejectsTruncatedPngAndRemovesStagingDirectory() throws IOException {
		assertMalformedImage(
				"broken.png",
				MediaType.IMAGE_PNG,
				Arrays.copyOf(createImage("png"), 20));
	}

	@Test
	void rejectsOversizedDimensionBeforeFullDecode() throws IOException {
		assertOversizedDimensions(6_001, 1);
	}

	@Test
	void rejectsOversizedPixelCountBeforeFullDecode() throws IOException {
		assertOversizedDimensions(4_000, 2_501);
	}

	private void assertOversizedDimensions(int width, int height) throws IOException {
		ImageStorage storage = new ImageStorage(this.temporaryDirectory.toString());
		ImageStorage.ImageBatch batch = storage.createBatch();
		byte[] oversizedPng = createPngHeader(width, height);

		Mono<Void> operation = Mono.usingWhen(
				Mono.just(batch),
				resource -> storage.stage(resource, List.of(filePart(
						"huge.png", MediaType.IMAGE_PNG, oversizedPng))),
				resource -> Mono.empty(),
				(resource, error) -> storage.cleanupFromRequest(resource, "test failure", error),
				resource -> storage.cleanupFromRequest(resource, "test cancellation", null));

		StepVerifier.create(operation)
				.expectErrorSatisfies(error -> assertThat(error)
						.isInstanceOf(ClaimApiException.class)
						.hasMessage("Image dimensions are too large"))
				.verify(Duration.ofSeconds(5));

		assertThat(stagingDirectory(batch)).doesNotExist();
	}

	private void assertMalformedImage(String filename, MediaType contentType, byte[] content) {
		ImageStorage storage = new ImageStorage(this.temporaryDirectory.toString());
		ImageStorage.ImageBatch batch = storage.createBatch();

		Mono<Void> operation = Mono.usingWhen(
				Mono.just(batch),
				resource -> storage.stage(resource, List.of(filePart(filename, contentType, content))),
				resource -> Mono.empty(),
				(resource, error) -> storage.cleanupFromRequest(resource, "test failure", error),
				resource -> storage.cleanupFromRequest(resource, "test cancellation", null));

		StepVerifier.create(operation)
				.expectErrorSatisfies(error -> assertThat(error)
						.isInstanceOf(ClaimApiException.class)
						.hasMessage("Image file is malformed"))
				.verify(Duration.ofSeconds(5));

		assertThat(stagingDirectory(batch)).doesNotExist();
	}

	private Path stagingDirectory(ImageStorage.ImageBatch batch) {
		return this.temporaryDirectory.resolve(".staging").resolve(batch.stagingId().toString());
	}

	private static byte[] createImage(String format) throws IOException {
		BufferedImage image = new BufferedImage(2, 2, BufferedImage.TYPE_INT_RGB);
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		if (!ImageIO.write(image, format, output)) {
			throw new IllegalStateException(format + " writer is unavailable");
		}
		return output.toByteArray();
	}

	private static byte[] createPngHeader(int width, int height) throws IOException {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try (DataOutputStream data = new DataOutputStream(output)) {
			data.write(new byte[] {(byte) 0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A});
			ByteArrayOutputStream header = new ByteArrayOutputStream();
			try (DataOutputStream headerData = new DataOutputStream(header)) {
				headerData.writeInt(width);
				headerData.writeInt(height);
				headerData.writeByte(8);
				headerData.writeByte(2);
				headerData.writeByte(0);
				headerData.writeByte(0);
				headerData.writeByte(0);
			}
			writePngChunk(data, "IHDR", header.toByteArray());
			writePngChunk(data, "IEND", new byte[0]);
		}
		return output.toByteArray();
	}

	private static void writePngChunk(DataOutputStream output, String type, byte[] content) throws IOException {
		byte[] typeBytes = type.getBytes(StandardCharsets.US_ASCII);
		CRC32 crc = new CRC32();
		crc.update(typeBytes);
		crc.update(content);
		output.writeInt(content.length);
		output.write(typeBytes);
		output.write(content);
		output.writeInt((int) crc.getValue());
	}

	private static FilePart filePart(String filename, MediaType contentType, byte[] content) {
		FilePart filePart = mock(FilePart.class);
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(contentType);
		when(filePart.filename()).thenReturn(filename);
		when(filePart.headers()).thenReturn(headers);
		when(filePart.transferTo(any(Path.class))).thenAnswer(invocation -> {
			Path destination = invocation.getArgument(0);
			return Mono.fromRunnable(() -> write(destination, content));
		});
		return filePart;
	}

	private static void write(Path destination, byte[] content) {
		try {
			Files.write(destination, content);
		}
		catch (IOException exception) {
			throw new UncheckedIOException(exception);
		}
	}
}
