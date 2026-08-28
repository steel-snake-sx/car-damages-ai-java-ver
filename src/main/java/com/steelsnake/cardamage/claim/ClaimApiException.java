package com.steelsnake.cardamage.claim;

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

	HttpStatus status() {
		return this.status;
	}
}
