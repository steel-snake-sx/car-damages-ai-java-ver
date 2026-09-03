package com.steelsnake.cardamage.claim;

import java.util.List;

// модель не является доверенным источником, поэтому проверку проходят обе реализации границы
public record DamageAnalysis(
		boolean carDetected,
		String summary,
		double confidence,
		List<DamageFinding> findings) {

	static final int MAX_SUMMARY_LENGTH = 1_000;
	static final int MAX_FINDINGS = 20;

	public DamageAnalysis {
		summary = requireText(summary, "summary", MAX_SUMMARY_LENGTH);
		confidence = requireConfidence(confidence, "confidence");
		if (findings == null) {
			throw DamageAnalysisException.invalidResult("findings must not be null");
		}
		if (findings.size() > MAX_FINDINGS) {
			throw DamageAnalysisException.invalidResult(
					"findings must not exceed " + MAX_FINDINGS + " items");
		}
		// без распознанной машины перечень повреждений не имеет смысла и не должен попасть в результат
		if (!carDetected && !findings.isEmpty()) {
			throw DamageAnalysisException.invalidResult(
					"findings must be empty when no car is detected");
		}
		findings = List.copyOf(findings);
	}

	static String requireText(String value, String fieldName, int maxLength) {
		if (value == null) {
			throw DamageAnalysisException.invalidResult(fieldName + " must not be null");
		}
		// управляющие символы модели не нужны, но перевод строки допустим внутри текста
		String normalized = value.replaceAll("[\\p{Cntrl}&&[^\n]]", " ").strip();
		if (normalized.isEmpty()) {
			throw DamageAnalysisException.invalidResult(fieldName + " must not be blank");
		}
		if (normalized.length() > maxLength) {
			throw DamageAnalysisException.invalidResult(
					fieldName + " must not exceed " + maxLength + " characters");
		}
		return normalized;
	}

	static double requireConfidence(double value, String fieldName) {
		if (!Double.isFinite(value) || value < 0 || value > 1) {
			throw DamageAnalysisException.invalidResult(fieldName + " must be between 0 and 1");
		}
		return value;
	}
}
