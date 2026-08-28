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

	private static final int FIRST_PRODUCTION_CAR_YEAR = 1886;

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

	public Mono<ClaimCreatedResponse> createClaim(
			String carBrand,
			String carModel,
			int carYear,
			Flux<FilePart> images) {
		String normalizedBrand = validateVehicleText("carBrand", carBrand);
		String normalizedModel = validateVehicleText("carModel", carModel);
		validateCarYear(carYear);

		return images.collectList()
				.flatMap(imageParts -> createClaim(normalizedBrand, normalizedModel, carYear, imageParts));
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

	private Mono<ClaimCreatedResponse> createClaim(
			String carBrand,
			String carModel,
			int carYear,
			List<FilePart> images) {
		if (images.isEmpty() || images.size() > 3) {
			return Mono.error(ClaimApiException.badRequest("Between 1 and 3 images are required"));
		}

		return Mono.usingWhen(
				Mono.fromSupplier(this.imageStorage::createStaging),
				stagedImages -> this.imageStorage.stage(stagedImages, images)
						.then(persistClaim(carBrand, carModel, carYear, stagedImages)),
				stagedImages -> Mono.empty(),
				(stagedImages, error) -> this.imageStorage.cleanupIfUnmanaged(
						stagedImages, "claim creation failed", error),
				stagedImages -> this.imageStorage.cleanupIfUnmanaged(
						stagedImages, "claim creation was cancelled", null));
	}

	private Mono<ClaimCreatedResponse> persistClaim(
			String carBrand,
			String carModel,
			int carYear,
			ImageStorage.StagedImages stagedImages) {
		Instant now = Instant.now();
		Claim claim = new Claim(null, carBrand, carModel, carYear, ClaimStatus.ANALYSIS_PENDING, now, now);

		return this.transactionalOperator.execute(transaction -> registerCleanupSynchronization(stagedImages)
				.then(this.claimRepository.save(claim))
				.flatMap(savedClaim -> this.imageStorage.finalizeClaim(stagedImages, savedClaim.id())
						.flatMap(storedImages -> saveImageMetadata(savedClaim, storedImages)
								.thenReturn(new ClaimCreatedResponse(savedClaim.id(), savedClaim.status())))))
				.single();
	}

	private Mono<Void> registerCleanupSynchronization(ImageStorage.StagedImages stagedImages) {
		return TransactionSynchronizationManager.forCurrentTransaction()
				.flatMap(manager -> {
					manager.registerSynchronization(new TransactionSynchronization() {
						@Override
						public Mono<Void> afterCompletion(int status) {
							return imageStorage.afterTransaction(stagedImages, status);
						}
					});
					if (!stagedImages.markTransactionManaged()) {
						return Mono.error(new IllegalStateException("Staged images were already released"));
					}
					return Mono.empty();
				});
	}

	private Mono<Void> saveImageMetadata(Claim claim, List<ImageStorage.StoredImage> storedImages) {
		return Flux.fromIterable(storedImages)
				.concatMap(stored -> this.claimImageRepository.save(new ClaimImage(
								null,
								claim.id(),
								stored.storagePath(),
								stored.originalFilename(),
								stored.contentType(),
								stored.sizeBytes(),
								Instant.now())))
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
		int latestAllowedYear = Year.now().getValue() + 1;
		if (carYear < FIRST_PRODUCTION_CAR_YEAR || carYear > latestAllowedYear) {
			throw ClaimApiException.badRequest(
					"carYear must be between " + FIRST_PRODUCTION_CAR_YEAR + " and " + latestAllowedYear);
		}
	}
}
