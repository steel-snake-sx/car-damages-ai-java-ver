package com.steelsnake.cardamage.claim;

import java.util.UUID;

import org.springframework.http.HttpStatus;

final class ClaimApiException extends RuntimeException {

	private final HttpStatus status;

	private ClaimApiException(HttpStatus status, String message) {
		super(message);
		this.status = status;
	}

	static ClaimApiException badRequest(String message) {
		return new ClaimApiException(HttpStatus.BAD_REQUEST, message);
	}

	static ClaimApiException notFound() {
		return new ClaimApiException(HttpStatus.NOT_FOUND, "Claim not found");
	}

	static ClaimApiException analysisDispatchUnavailable(UUID claimId) {
		return new ClaimApiException(
				HttpStatus.SERVICE_UNAVAILABLE,
				"Claim " + claimId + " was saved but its analysis request could not be dispatched");
	}

	HttpStatus status() {
		return this.status;
	}
}
