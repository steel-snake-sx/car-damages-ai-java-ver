package com.steelsnake.cardamage.claim;

public enum DamageSeverity {
	LOW,
	MEDIUM,
	HIGH;

	// модель отдаёт severity строкой из enum схемы, но доверять ей всё равно нельзя
	static DamageSeverity from(String value) {
		if (value != null) {
			for (DamageSeverity severity : values()) {
				if (severity.name().equalsIgnoreCase(value.strip())) {
					return severity;
				}
			}
		}
		throw DamageAnalysisException.invalidResult("severity is not one of LOW/MEDIUM/HIGH");
	}
}
