package com.steelsnake.cardamage.claim;

import java.time.Instant;

import org.springframework.core.codec.DecodingException;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice(assignableTypes = ClaimController.class)
class ClaimExceptionHandler {

	@ExceptionHandler(ClaimApiException.class)
	ResponseEntity<ApiError> handleClaimApiException(ClaimApiException exception) {
		return errorResponse(exception.status(), exception.getMessage());
	}

	@ExceptionHandler(DataBufferLimitException.class)
	ResponseEntity<ApiError> handleMultipartSizeLimit(DataBufferLimitException exception) {
		return errorResponse(HttpStatus.CONTENT_TOO_LARGE, "Multipart request exceeds configured limits");
	}

	@ExceptionHandler(DecodingException.class)
	ResponseEntity<ApiError> handleMultipartDecoding(DecodingException exception) {
		return multipartError(exception);
	}

	private static ResponseEntity<ApiError> multipartError(Throwable exception) {
		if (isMultipartLimit(exception)) {
			return errorResponse(HttpStatus.CONTENT_TOO_LARGE, "Multipart request exceeds configured limits");
		}
		return errorResponse(HttpStatus.BAD_REQUEST, "Malformed multipart request");
	}

	private static boolean isMultipartLimit(Throwable exception) {
		for (Throwable current = exception; current != null; current = current.getCause()) {
			if (current instanceof DataBufferLimitException
					|| current.getMessage() != null && current.getMessage().startsWith("Too many parts")) {
				return true;
			}
		}
		return false;
	}

	private static ResponseEntity<ApiError> errorResponse(HttpStatus status, String message) {
		ApiError error = new ApiError(status.value(), status.getReasonPhrase(), message, Instant.now());
		return ResponseEntity.status(status).body(error);
	}

	record ApiError(int status, String error, String message, Instant timestamp) {
	}
}
