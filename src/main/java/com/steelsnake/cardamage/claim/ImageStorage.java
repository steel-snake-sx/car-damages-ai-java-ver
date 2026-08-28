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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import javax.imageio.IIOException;
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

	StagedImages createStaging() {
		return new StagedImages(UUID.randomUUID());
	}

	Mono<Void> stage(StagedImages stagedImages, List<FilePart> fileParts) {
		Path stagingDirectory = stagingDirectory(stagedImages);

		Mono<Void> operation = createDirectory(stagingDirectory)
				.thenMany(reactor.core.publisher.Flux.fromIterable(fileParts)
						.concatMap(filePart -> stageFile(stagingDirectory, filePart)))
				.collectList()
				.doOnNext(stagedImages::setImages)
				.then()
				.cache();
		stagedImages.trackFilesystemOperation(operation);
		return operation;
	}

	Mono<List<StoredImage>> finalizeClaim(StagedImages stagedImages, UUID claimId) {
		Path stagingDirectory = stagingDirectory(stagedImages);
		Path claimDirectory = claimDirectory(claimId);
		Mono<List<StoredImage>> operation = Mono.fromCallable(() -> {
			try {
				Files.createDirectories(this.rootDirectory);
				try {
					Files.move(stagingDirectory, claimDirectory, StandardCopyOption.ATOMIC_MOVE);
				}
				catch (AtomicMoveNotSupportedException exception) {
					Files.move(stagingDirectory, claimDirectory);
				}
			}
			catch (IOException exception) {
				throw new UncheckedIOException(exception);
			}
			stagedImages.setClaimId(claimId);
			return stagedImages.images().stream()
					.map(image -> new StoredImage(
							(claimId + "/" + image.storedFilename()).replace('\\', '/'),
							image.originalFilename(),
							image.contentType(),
							image.sizeBytes()))
					.toList();
		}).subscribeOn(Schedulers.boundedElastic()).cache();
		stagedImages.trackFilesystemOperation(operation.then());
		return operation;
	}

	Mono<Void> cleanupIfUnmanaged(StagedImages stagedImages, String reason, Throwable primaryError) {
		return stagedImages.beginUnmanagedCleanup()
				? cleanup(stagedImages, reason, primaryError)
				: Mono.empty();
	}

	Mono<Void> afterTransaction(StagedImages stagedImages, int status) {
		if (status == TransactionSynchronization.STATUS_COMMITTED) {
			stagedImages.markCommitted();
			return Mono.empty();
		}
		if (status == TransactionSynchronization.STATUS_UNKNOWN) {
			stagedImages.markOutcomeUnknown();
			logger.warn(
					"Keeping image files for staging id {} and claim id {} because transaction outcome is unknown",
					stagedImages.stagingId(), stagedImages.claimId());
			return Mono.empty();
		}
		return stagedImages.beginTransactionCleanup()
				? cleanup(stagedImages, "transaction completed with status " + status, null)
				: Mono.empty();
	}

	private Mono<Void> cleanup(StagedImages stagedImages, String reason, Throwable primaryError) {
		return stagedImages.awaitFilesystemOperation()
				.onErrorResume(error -> Mono.empty())
				.then(Mono.fromRunnable(() -> deleteStagedAndFinalDirectories(stagedImages)))
				.subscribeOn(Schedulers.boundedElastic())
				.then()
				.onErrorResume(cleanupError -> {
					if (primaryError != null) {
						primaryError.addSuppressed(cleanupError);
					}
					logger.error(
							"Failed to clean image files for staging id {} and claim id {} after {}",
							stagedImages.stagingId(), stagedImages.claimId(), reason, cleanupError);
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
		return Mono.fromRunnable(() -> {
			try {
				Files.createDirectories(directory);
			}
			catch (IOException exception) {
				throw new UncheckedIOException(exception);
			}
		}).subscribeOn(Schedulers.boundedElastic()).then();
	}

	private Mono<Void> transfer(FilePart filePart, Path destination) {
		return Mono.defer(() -> filePart.transferTo(destination))
				.subscribeOn(Schedulers.boundedElastic());
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
		boolean acquired = false;
		try {
			this.imageValidationPermit.acquire();
			acquired = true;
			validateImage(path, expectedType);
		}
		catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Image validation was interrupted", exception);
		}
		finally {
			if (acquired) {
				this.imageValidationPermit.release();
			}
		}
	}

	private void deleteStagedAndFinalDirectories(StagedImages stagedImages) {
		List<RuntimeException> failures = new ArrayList<>();
		deleteRecursively(stagingDirectory(stagedImages), failures);
		if (stagedImages.claimId() != null) {
			deleteRecursively(claimDirectory(stagedImages.claimId()), failures);
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

	private Path stagingDirectory(StagedImages stagedImages) {
		return resolveWithinRoot(".staging/" + stagedImages.stagingId());
	}

	private Path claimDirectory(UUID claimId) {
		return resolveWithinRoot(claimId.toString());
	}

	private Path resolveWithinRoot(String relativePath) {
		Path resolved = this.rootDirectory.resolve(relativePath).normalize();
		if (!resolved.startsWith(this.rootDirectory)) {
			throw new IllegalArgumentException("Storage path escapes the configured image directory");
		}
		return resolved;
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
			catch (IIOException | IllegalArgumentException exception) {
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
		return safeName.length() <= 255 ? safeName : safeName.substring(0, 255);
	}

	record StoredImage(String storagePath, String originalFilename, String contentType, long sizeBytes) {
	}

	private record StagedImage(String storedFilename, String originalFilename, String contentType, long sizeBytes) {
	}

	static final class StagedImages {

		private static final int UNMANAGED = 0;
		private static final int TRANSACTION_MANAGED = 1;
		private static final int CLEANUP_STARTED = 2;
		private static final int COMMITTED = 3;
		private static final int OUTCOME_UNKNOWN = 4;

		private final UUID stagingId;
		private final AtomicInteger state = new AtomicInteger(UNMANAGED);
		private volatile List<StagedImage> images = List.of();
		private volatile UUID claimId;
		private volatile Mono<Void> filesystemOperation = Mono.empty();

		StagedImages(UUID stagingId) {
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

		void trackFilesystemOperation(Mono<Void> operation) {
			this.filesystemOperation = operation;
		}

		Mono<Void> awaitFilesystemOperation() {
			return this.filesystemOperation;
		}

		boolean markTransactionManaged() {
			return this.state.compareAndSet(UNMANAGED, TRANSACTION_MANAGED);
		}

		boolean beginUnmanagedCleanup() {
			return this.state.compareAndSet(UNMANAGED, CLEANUP_STARTED);
		}

		boolean beginTransactionCleanup() {
			return this.state.compareAndSet(TRANSACTION_MANAGED, CLEANUP_STARTED);
		}

		void markCommitted() {
			this.state.compareAndSet(TRANSACTION_MANAGED, COMMITTED);
		}

		void markOutcomeUnknown() {
			this.state.compareAndSet(TRANSACTION_MANAGED, OUTCOME_UNKNOWN);
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
