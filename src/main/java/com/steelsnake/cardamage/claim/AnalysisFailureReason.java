package com.steelsnake.cardamage.claim;

// короткая категория отказа для admin/debug: сырые ошибки провайдера и стектрейсы в БД не попадают
public enum AnalysisFailureReason {

	AI_UNAVAILABLE(true),
	AI_REQUEST_REJECTED(false),
	INVALID_AI_RESULT(false);

	private final boolean retryable;

	AnalysisFailureReason(boolean retryable) {
		this.retryable = retryable;
	}

	boolean retryable() {
		return this.retryable;
	}}
