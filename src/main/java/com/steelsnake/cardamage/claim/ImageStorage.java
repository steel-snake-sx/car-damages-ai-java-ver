package com.steelsnake.cardamage.claim;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionSynchronization;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Component
public class ImageStorage {

	public static final long MAX_IMAGE_SIZE_BYTES = 10L * 1024 * 1024;
	private static final int MAX_IMAGE_DIMENSION = 6_000;
	private static final long MAX_IMAGE_PIXELS = 10_000_000;
	private static final Logger logger = LoggerFactory.getLogger(ImageStorage.class);

	private final Path rootDirectory;
	private final Semaphore imageValidationPermit = new Semaphore(1);

	ImageStorage(@Value("${app.image-storage.directory}") String rootDirectory) {
		this.rootDirectory = Path.of(rootDirectory).toAbsolutePath().normalize();
	}

	ImageBatch createBatch() {
		return new ImageBatch(UUID.randomUUID());
	}

	Mono<Void> stage(ImageBatch batch, List<FilePart> fileParts) {
		Path stagingDirectory = stagingDirectory(batch);

		Mono<Void> operation = createDirectory(stagingDirectory)
				.thenMany(Flux.fromIterable(fileParts)
						.concatMap(filePart -> stageFile(stagingDirectory, filePart)))
				.collectList()
				.doOnNext(batch::setImages)
				.then()
				.cache();
		if (!batch.trackRequestFilesystemOperation(operation)) {
			return Mono.error(new IllegalStateException("Image cleanup was already claimed"));
		}
		return operation;
	}

	Mono<List<StoredImage>> moveToClaim(ImageBatch batch, UUID claimId) {
		Path stagingDirectory = stagingDirectory(batch);
		Path claimDirectory = claimDirectory(claimId);
		Mono<List<StoredImage>> operation = Mono.fromCallable(() -> {
			if (Files.exists(claimDirectory)) {
				throw new IllegalStateException("Image directory already exists for claim " + claimId);
			}
			try {
				try {
					Files.move(stagingDirectory, claimDirectory, StandardCopyOption.ATOMIC_MOVE);
					batch.setClaimId(claimId);
				}
				catch (AtomicMoveNotSupportedException exception) {
					batch.setClaimId(claimId);
					Files.move(stagingDirectory, claimDirectory);
				}
			}
			catch (IOException exception) {
				throw new UncheckedIOException(exception);
			}
			return batch.images().stream()
					.map(image -> new StoredImage(
							(claimId + "/" + image.storedFilename()).replace('\\', '/'),
							image.originalFilename(),
							image.contentType(),
							image.sizeBytes()))
					.toList();
		}).subscribeOn(Schedulers.boundedElastic()).cache();
		if (!batch.trackTransactionFilesystemOperation(operation.then())) {
			return Mono.error(new IllegalStateException("Image cleanup was already claimed"));
		}
		return operation;
	}

	Mono<Void> cleanupFromRequest(ImageBatch batch, String reason, Throwable primaryError) {
		return batch.beginRequestCleanup()
				? cleanup(batch, reason, primaryError)
				: Mono.empty();
	}

	Mono<Void> completeTransaction(ImageBatch batch, int status) {
		if (status == TransactionSynchronization.STATUS_COMMITTED) {
			batch.keepFiles();
			return Mono.empty();
		}
		if (status == TransactionSynchronization.STATUS_UNKNOWN) {
			batch.keepFiles();
			logger.warn(
					"Keeping image files for staging id {} and claim id {} because transaction outcome is unknown",
					batch.stagingId(), batch.claimId());
			return Mono.empty();
		}
		return batch.beginTransactionCleanup()
				? cleanup(batch, "transaction completed with status " + status, null)
				: Mono.empty();
	}

	private Mono<Void> cleanup(ImageBatch batch, String reason, Throwable primaryError) {
		return batch.awaitFilesystemOperation()
				.onErrorComplete()
				.then(Mono.<Void>fromRunnable(() -> deleteStagedAndFinalDirectories(batch))
						.subscribeOn(Schedulers.boundedElastic()))
				.onErrorResume(cleanupError -> {
					if (primaryError != null) {
						primaryError.addSuppressed(cleanupError);
					}
					logger.error(
							"Failed to clean image files for staging id {} and claim id {} after {}",
							batch.stagingId(), batch.claimId(), reason, cleanupError);
					return Mono.empty();
				});
	}

	private Mono<StagedImage> stageFile(Path stagingDirectory, FilePart filePart) {
		ImageType imageType = supportedImageType(filePart);
		String storedFilename = UUID.randomUUID() + imageType.extension();
		Path destination = stagingDirectory.resolve(storedFilename).normalize();

		return transfer(filePart, destination)
				.then(inspectStoredFile(destination, storedFilename, filePart.filename(), imageType));
	}

	private Mono<Void> createDirectory(Path directory) {
		return Mono.<Void>fromRunnable(() -> {
			try {
				Files.createDirectories(directory);
			}
			catch (IOException exception) {
				throw new UncheckedIOException(exception);
			}
		}).subscribeOn(Schedulers.boundedElastic());
	}

	private Mono<Void> transfer(FilePart filePart, Path destination) {
		return filePart.transferTo(destination).subscribeOn(Schedulers.boundedElastic());
	}

	private Mono<StagedImage> inspectStoredFile(
			Path destination,
			String storedFilename,
			String originalFilename,
			ImageType imageType) {
		return Mono.fromCallable(() -> {
			long size = Files.size(destination);
			if (size == 0) {
				throw ClaimApiException.badRequest("Images must not be empty");
			}
			if (size > MAX_IMAGE_SIZE_BYTES) {
				throw ClaimApiException.badRequest("Each image must not exceed 10 MB");
			}
			validateImageWithPermit(destination, imageType);
			return new StagedImage(
					storedFilename,
					sanitizeFilename(originalFilename),
					imageType.contentType(),
					size);
		}).subscribeOn(Schedulers.boundedElastic());
	}

	private void validateImageWithPermit(Path path, ImageType expectedType) throws IOException {
		try {
			this.imageValidationPermit.acquire();
		}
		catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Image validation was interrupted", exception);
		}
		try {
			validateImage(path, expectedType);
		}
		finally {
			this.imageValidationPermit.release();
		}
	}

	private void deleteStagedAndFinalDirectories(ImageBatch batch) {
		List<RuntimeException> failures = new ArrayList<>();
		deleteRecursively(stagingDirectory(batch), failures);
		if (batch.claimId() != null) {
			deleteRecursively(claimDirectory(batch.claimId()), failures);
		}
		if (!failures.isEmpty()) {
			RuntimeException failure = failures.getFirst();
			failures.stream().skip(1).forEach(failure::addSuppressed);
			throw failure;
		}
	}

	private void deleteRecursively(Path directory, List<RuntimeException> failures) {
		if (!Files.exists(directory)) {
			return;
		}
		try (Stream<Path> paths = Files.walk(directory)) {
			for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
				try {
					Files.deleteIfExists(path);
				}
				catch (IOException exception) {
					failures.add(new UncheckedIOException(exception));
				}
			}
		}
		catch (IOException exception) {
			failures.add(new UncheckedIOException(exception));
		}
		catch (UncheckedIOException exception) {
			failures.add(exception);
		}
	}

	private Path stagingDirectory(ImageBatch batch) {
		return this.rootDirectory.resolve(".staging").resolve(batch.stagingId().toString());
	}

	private Path claimDirectory(UUID claimId) {
		return this.rootDirectory.resolve(claimId.toString());
	}

	private static ImageType supportedImageType(FilePart filePart) {
		MediaType contentType = filePart.headers().getContentType();
		if (contentType != null && "image".equalsIgnoreCase(contentType.getType())) {
			if ("jpeg".equalsIgnoreCase(contentType.getSubtype())) {
				return ImageType.JPEG;
			}
			if ("png".equalsIgnoreCase(contentType.getSubtype())) {
				return ImageType.PNG;
			}
		}
		throw ClaimApiException.badRequest("Only JPEG and PNG images are supported");
	}

	private static void validateImage(Path path, ImageType expectedType) throws IOException {
		try (ImageInputStream input = ImageIO.createImageInputStream(path.toFile())) {
			if (input == null) {
				throw ClaimApiException.badRequest("Image file is malformed");
			}

			Iterator<ImageReader> readers = ImageIO.getImageReaders(input);
			if (!readers.hasNext()) {
				throw ClaimApiException.badRequest("Image file is malformed");
			}

			ImageReader reader = readers.next();
			try {
				AtomicBoolean decodingWarning = new AtomicBoolean();
				reader.addIIOReadWarningListener((source, warning) -> decodingWarning.set(true));
				if (!expectedType.formatName().equalsIgnoreCase(reader.getFormatName())) {
					throw ClaimApiException.badRequest("Image content does not match its content type");
				}
				reader.setInput(input, true, true);
				int width = reader.getWidth(0);
				int height = reader.getHeight(0);
				if (width <= 0 || height <= 0
						|| width > MAX_IMAGE_DIMENSION || height > MAX_IMAGE_DIMENSION
						|| (long) width * height > MAX_IMAGE_PIXELS) {
					throw ClaimApiException.badRequest("Image dimensions are too large");
				}
				if (reader.read(0) == null || decodingWarning.get()) {
					throw ClaimApiException.badRequest("Image file is malformed");
				}
			}
			catch (IOException | IllegalArgumentException | IndexOutOfBoundsException exception) {
				throw ClaimApiException.badRequest("Image file is malformed");
			}
			finally {
				reader.dispose();
			}
		}
	}

	private static String sanitizeFilename(String filename) {
		String safeName = filename == null ? "image" : filename.replace('\\', '/');
		int lastSlash = safeName.lastIndexOf('/');
		if (lastSlash >= 0) {
			safeName = safeName.substring(lastSlash + 1);
		}
		safeName = safeName.replaceAll("[\\p{Cntrl}]", "_").strip();
		if (safeName.isEmpty()) {
			safeName = "image";
		}
		if (safeName.codePointCount(0, safeName.length()) > 255) {
			safeName = safeName.substring(0, safeName.offsetByCodePoints(0, 255));
		}
		return safeName;
	}

	record StoredImage(String storagePath, String originalFilename, String contentType, long sizeBytes) {
	}

	private record StagedImage(String storedFilename, String originalFilename, String contentType, long sizeBytes) {
	}

	static final class ImageBatch {

		private final UUID stagingId;
		// State and operation share this monitor so cleanup cannot miss newly registered work.
		private State state = State.REQUEST;
		private volatile List<StagedImage> images = List.of();
		private volatile UUID claimId;
		// Cached filesystem work survives cancellation so cleanup can join it before deleting.
		private Mono<Void> filesystemOperation = Mono.empty();

		ImageBatch(UUID stagingId) {
			this.stagingId = stagingId;
		}

		UUID stagingId() {
			return this.stagingId;
		}

		List<StagedImage> images() {
			return this.images;
		}

		void setImages(List<StagedImage> images) {
			this.images = List.copyOf(images);
		}

		UUID claimId() {
			return this.claimId;
		}

		void setClaimId(UUID claimId) {
			this.claimId = claimId;
		}

		boolean trackRequestFilesystemOperation(Mono<Void> operation) {
			return trackFilesystemOperation(State.REQUEST, operation);
		}

		boolean trackTransactionFilesystemOperation(Mono<Void> operation) {
			return trackFilesystemOperation(State.TRANSACTION, operation);
		}

		synchronized Mono<Void> awaitFilesystemOperation() {
			return this.filesystemOperation;
		}

		synchronized boolean beginTransaction() {
			return transition(State.REQUEST, State.TRANSACTION);
		}

		synchronized boolean beginRequestCleanup() {
			return transition(State.REQUEST, State.CLEANING);
		}

		synchronized boolean beginTransactionCleanup() {
			return transition(State.TRANSACTION, State.CLEANING);
		}

		synchronized void keepFiles() {
			transition(State.TRANSACTION, State.KEPT);
		}

		private synchronized boolean trackFilesystemOperation(State expectedState, Mono<Void> operation) {
			if (this.state != expectedState) {
				return false;
			}
			this.filesystemOperation = operation;
			return true;
		}

		private boolean transition(State expectedState, State nextState) {
			if (this.state != expectedState) {
				return false;
			}
			this.state = nextState;
			return true;
		}

		private enum State {
			REQUEST,
			TRANSACTION,
			CLEANING,
			KEPT
		}
	}

	private enum ImageType {
		JPEG(".jpg", "image/jpeg", "JPEG"),
		PNG(".png", "image/png", "PNG");

		private final String extension;
		private final String contentType;
		private final String formatName;

		ImageType(String extension, String contentType, String formatName) {
			this.extension = extension;
			this.contentType = contentType;
			this.formatName = formatName;
		}

		String extension() {
			return this.extension;
		}

		String contentType() {
			return this.contentType;
		}

		String formatName() {
			return this.formatName;
		}
	}
}
