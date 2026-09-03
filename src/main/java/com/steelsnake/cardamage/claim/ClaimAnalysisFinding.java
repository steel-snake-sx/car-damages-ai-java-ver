package com.steelsnake.cardamage.claim;

import java.util.UUID;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("claim_analysis_findings")
public record ClaimAnalysisFinding(
		@Id UUID id,
		UUID claimId,
		// порядок из ответа модели сохраняем явно, иначе выборка вернёт произвольную последовательность
		short position,
		String partName,
		String description,
		DamageSeverity severity,
		double confidence) {
}
