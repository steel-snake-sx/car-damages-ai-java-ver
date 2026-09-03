package com.steelsnake.cardamage.claim;

// единственный тип ошибки границы анализа: реализация не выпускает наружу собственные транспортные исключения
public class DamageAnalysisException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	private final AnalysisFailureReason reason;

	private DamageAnalysisException(AnalysisFailureReason reason, String message, Throwable cause) {
		super(message, cause);
		this.reason = reason;
	}

	// фабрики публичные, потому что реализации DamageAnalyzer сообщают об отказе только через них
	public static DamageAnalysisException unavailable(String message, Throwable cause) {
		return new DamageAnalysisException(AnalysisFailureReason.AI_UNAVAILABLE, message, cause);
	}

	public static DamageAnalysisException requestRejected(String message, Throwable cause) {
		return new DamageAnalysisException(AnalysisFailureReason.AI_REQUEST_REJECTED, message, cause);
	}

	public static DamageAnalysisException invalidResult(String message) {
		return new DamageAnalysisException(AnalysisFailureReason.INVALID_AI_RESULT, message, null);
	}

	public AnalysisFailureReason reason() {
		return this.reason;
	}
}
