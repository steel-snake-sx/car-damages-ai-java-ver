package com.steelsnake.cardamage.claim;

public record DamageFinding(
		String partName,
		String description,
		DamageSeverity severity,
		double confidence) {

	static final int MAX_PART_NAME_LENGTH = 120;
	static final int MAX_DESCRIPTION_LENGTH = 500;

	public DamageFinding {
		partName = DamageAnalysis.requireText(partName, "partName", MAX_PART_NAME_LENGTH);
		description = DamageAnalysis.requireText(description, "description", MAX_DESCRIPTION_LENGTH);
		if (severity == null) {
			throw DamageAnalysisException.invalidResult("severity must not be null");
		}
		confidence = DamageAnalysis.requireConfidence(confidence, "finding confidence");
	}
}
