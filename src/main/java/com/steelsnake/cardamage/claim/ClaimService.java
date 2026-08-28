package com.steelsnake.cardamage.claim;

import java.time.Instant;
import java.time.Year;
import java.util.List;
import java.util.UUID;

import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionSynchronization;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.reactive.TransactionalOperator;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class ClaimService {

	private static final int MIN_CAR_YEAR = 1886;

	private final ClaimRepository claimRepository;
	private final ClaimImageRepository claimImageRepository;
	private final ImageStorage imageStorage;
	private final TransactionalOperator transactionalOperator;

	ClaimService(
			ClaimRepository claimRepository,
			ClaimImageRepository claimImageRepository,
			ImageStorage imageStorage,
			TransactionalOperator transactionalOperator) {
		this.claimRepository = claimRepository;
		this.claimImageRepository = claimImageRepository;
		this.imageStorage = imageStorage;
		this.transactionalOperator = transactionalOperator;
	}

	public Mono<ClaimStatusResponse> createClaim(
			String brand, String model, int year, List<FilePart> images) {
		String cleanBrand = validateVehicleText("carBrand", brand);
		String cleanModel = validateVehicleText("carModel", model);
		validateCarYear(year);

		if (images.isEmpty() || images.size() > 3) {
			return Mono.error(ClaimApiException.badRequest("Between 1 and 3 images are required"));
		}

		return Mono.usingWhen(
				Mono.fromSupplier(this.imageStorage::createBatch),
				batch -> this.imageStorage.stage(batch, images)
						.then(Mono.defer(() -> saveClaim(cleanBrand, cleanModel, year, batch))),
				batch -> Mono.empty(),
				(batch, error) -> this.imageStorage.cleanupFromRequest(
						batch, "claim creation failed", error),
				batch -> this.imageStorage.cleanupFromRequest(
						batch, "claim creation was cancelled", null));
	}

	public Mono<ClaimStatusResponse> getStatus(UUID claimId) {
		return this.claimRepository.findById(claimId)
				.map(claim -> new ClaimStatusResponse(claim.id(), claim.status()))
				.switchIfEmpty(Mono.error(ClaimApiException.notFound()));
	}

	public Flux<AdminClaimSummary> getAdminClaims() {
		return this.claimRepository.findAllByOrderByCreatedAtDesc()
				.map(AdminClaimSummary::from);
	}

	public Mono<AdminClaimDetails> getAdminClaim(UUID claimId) {
		return this.claimRepository.findById(claimId)
				.map(AdminClaimDetails::from)
				.switchIfEmpty(Mono.error(ClaimApiException.notFound()));
	}

	private Mono<ClaimStatusResponse> saveClaim(
			String brand, String model, int year, ImageStorage.ImageBatch batch) {
		Instant now = Instant.now();
		Claim claim = new Claim(null, brand, model, year, ClaimStatus.ANALYSIS_PENDING, now, now);

		Mono<ClaimStatusResponse> save = registerImageCleanup(batch)
				.then(this.claimRepository.save(claim))
				.flatMap(saved -> this.imageStorage.moveToClaim(batch, saved.id())
						.flatMap(images -> saveImages(saved, images)
								.thenReturn(new ClaimStatusResponse(saved.id(), saved.status()))));
		return this.transactionalOperator.transactional(save);
	}

	private Mono<Void> registerImageCleanup(ImageStorage.ImageBatch batch) {
		return TransactionSynchronizationManager.forCurrentTransaction()
				.flatMap(manager -> {
					manager.registerSynchronization(new TransactionSynchronization() {
						@Override
						public Mono<Void> afterCompletion(int status) {
							return imageStorage.completeTransaction(batch, status);
						}
					});
					if (!batch.beginTransaction()) {
						return Mono.error(new IllegalStateException("Image cleanup was already claimed"));
					}
					return Mono.empty();
				});
	}

	private Mono<Void> saveImages(Claim claim, List<ImageStorage.StoredImage> images) {
		return Flux.fromIterable(images)
				.concatMap(image -> this.claimImageRepository.save(new ClaimImage(
								null,
								claim.id(),
								image.storagePath(),
								image.originalFilename(),
								image.contentType(),
								image.sizeBytes(),
								claim.createdAt())))
				.then();
	}

	private static String validateVehicleText(String fieldName, String value) {
		String candidate = value == null ? "" : value;
		if (candidate.chars().anyMatch(Character::isISOControl)) {
			throw ClaimApiException.badRequest(fieldName + " must not contain control characters");
		}
		String normalized = candidate.strip();
		if (normalized.isEmpty()) {
			throw ClaimApiException.badRequest(fieldName + " must not be blank");
		}
		if (normalized.length() > 80) {
			throw ClaimApiException.badRequest(fieldName + " must not exceed 80 characters");
		}
		return normalized;
	}

	private static void validateCarYear(int carYear) {
		int maxYear = Year.now().getValue() + 1;
		if (carYear < MIN_CAR_YEAR || carYear > maxYear) {
			throw ClaimApiException.badRequest(
					"carYear must be between " + MIN_CAR_YEAR + " and " + maxYear);
		}
	}
}
