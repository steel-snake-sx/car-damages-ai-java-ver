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

import reactor.core.publisher.Mono;

import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.SchemaProperty;

@RestController
@RequestMapping("/api/claims")
public class ClaimController {

	private static final Set<String> EXPECTED_PARTS = Set.of("carBrand", "carModel", "carYear", "images");

	private final ClaimService claimService;

	ClaimController(ClaimService claimService) {
		this.claimService = claimService;
	}

	@PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
	@io.swagger.v3.oas.annotations.parameters.RequestBody(
			required = true,
			content = @Content(
					mediaType = MediaType.MULTIPART_FORM_DATA_VALUE,
					schema = @Schema(
							type = "object",
							requiredProperties = {"carBrand", "carModel", "carYear", "images"}),
					schemaProperties = {
							@SchemaProperty(name = "carBrand", schema = @Schema(type = "string")),
							@SchemaProperty(name = "carModel", schema = @Schema(type = "string")),
							@SchemaProperty(name = "carYear", schema = @Schema(type = "integer", format = "int32")),
							@SchemaProperty(name = "images", array = @ArraySchema(
									minItems = 1,
									maxItems = 3,
									schema = @Schema(type = "string", format = "binary")))
				}))
	public Mono<ResponseEntity<ClaimStatusResponse>> createClaim(
			@RequestBody Mono<MultiValueMap<String, Part>> multipartBody) {
		return multipartBody.flatMap(parts -> {
			validatePartNames(parts);
			String brand = requiredFormField(parts, "carBrand").value();
			String model = requiredFormField(parts, "carModel").value();
			String year = requiredFormField(parts, "carYear").value();
			List<FilePart> images = requiredImages(parts);

			return this.claimService.createClaim(
					brand, model, parseCarYear(year), images);
		})
				.map(response -> ResponseEntity
						.accepted()
						.location(URI.create("/api/claims/" + response.id() + "/status"))
						.body(response));
	}

	@GetMapping("/{id}/status")
	public Mono<ClaimStatusResponse> getStatus(@PathVariable UUID id) {
		return this.claimService.getStatus(id);
	}

	private static void validatePartNames(MultiValueMap<String, Part> parts) {
		for (String name : parts.keySet()) {
			if (!EXPECTED_PARTS.contains(name)) {
				throw ClaimApiException.badRequest("Unexpected multipart part: " + name);
			}
		}
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

	private static List<FilePart> requiredImages(MultiValueMap<String, Part> parts) {
		List<Part> values = parts.get("images");
		if (values == null || values.isEmpty()) {
			throw ClaimApiException.badRequest("Between 1 and 3 images are required");
		}
		return values.stream()
				.map(part -> {
					if (!(part instanceof FilePart filePart)) {
						throw ClaimApiException.badRequest("images must contain files");
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
