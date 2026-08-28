package com.steelsnake.cardamage.claim;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.http.codec.multipart.FormFieldPart;
import org.springframework.http.codec.multipart.Part;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/claims")
public class ClaimController {

	private static final Set<String> EXPECTED_PARTS = Set.of("carBrand", "carModel", "carYear", "images");

	private final ClaimService claimService;

	ClaimController(ClaimService claimService) {
		this.claimService = claimService;
	}

	@PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
	public Mono<ResponseEntity<ClaimCreatedResponse>> createClaim(
			@RequestBody Mono<MultiValueMap<String, Part>> multipartBody) {
		return multipartBody.flatMap(parts -> {
			validatePartNames(parts);
			FormFieldPart carBrand = requiredFormField(parts, "carBrand");
			FormFieldPart carModel = requiredFormField(parts, "carModel");
			FormFieldPart carYear = requiredFormField(parts, "carYear");
			List<FilePart> images = requiredFiles(parts, "images");

			return this.claimService.createClaim(
					carBrand.value(),
					carModel.value(),
					parseCarYear(carYear.value()),
					Flux.fromIterable(images));
		})
				.map(response -> ResponseEntity
						.created(URI.create("/api/claims/" + response.id() + "/status"))
						.body(response));
	}

	@GetMapping("/{id}/status")
	public Mono<ClaimStatusResponse> getStatus(@PathVariable UUID id) {
		return this.claimService.getStatus(id);
	}

	private static void validatePartNames(MultiValueMap<String, Part> parts) {
		parts.keySet().stream()
				.filter(name -> !EXPECTED_PARTS.contains(name))
				.findFirst()
				.ifPresent(name -> {
					throw ClaimApiException.badRequest("Unexpected multipart part: " + name);
				});
	}

	private static FormFieldPart requiredFormField(MultiValueMap<String, Part> parts, String name) {
		List<Part> values = parts.get(name);
		if (values == null || values.isEmpty()) {
			throw ClaimApiException.badRequest(name + " is required");
		}
		if (values.size() != 1) {
			throw ClaimApiException.badRequest(name + " must be provided exactly once");
		}
		if (!(values.getFirst() instanceof FormFieldPart formField)) {
			throw ClaimApiException.badRequest(name + " must be a form field");
		}
		return formField;
	}

	private static List<FilePart> requiredFiles(MultiValueMap<String, Part> parts, String name) {
		List<Part> values = parts.get(name);
		if (values == null || values.isEmpty()) {
			throw ClaimApiException.badRequest("Between 1 and 3 images are required");
		}
		return values.stream()
				.map(part -> {
					if (!(part instanceof FilePart filePart)) {
						throw ClaimApiException.badRequest(name + " must contain files");
					}
					return filePart;
				})
				.toList();
	}

	private static int parseCarYear(String carYear) {
		try {
			return Integer.parseInt(carYear.strip());
		}
		catch (NumberFormatException exception) {
			throw ClaimApiException.badRequest("carYear must be a whole number");
		}
	}
}
