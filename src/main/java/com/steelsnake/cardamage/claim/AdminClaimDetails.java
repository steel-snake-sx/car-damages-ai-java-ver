package com.steelsnake.cardamage.claim;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record AdminClaimDetails(
		UUID id,
		String carBrand,
		String carModel,
		int carYear,
		ClaimStatus status,
		AnalysisFailureReason analysisFailureReason,
		Instant createdAt,
		Instant updatedAt,
		Analysis analysis) {

	static AdminClaimDetails from(Claim claim, Analysis analysis) {
		return new AdminClaimDetails(
				claim.id(),
				claim.carBrand(),
				claim.carModel(),
				claim.carYear(),
				claim.status(),
				claim.analysisFailureReason(),
				claim.createdAt(),
				claim.updatedAt(),
				analysis);
	}

	public record Analysis(
			boolean carDetected,
			String summary,
			double confidence,
			Instant createdAt,
			List<Finding> findings) {

		static Analysis from(ClaimAnalysis analysis, List<ClaimAnalysisFinding> findings) {
			return new Analysis(
					analysis.carDetected(),
					analysis.summary(),
					analysis.confidence(),
					analysis.createdAt(),
					findings.stream().map(Finding::from).toList());
		}
	}

	public record Finding(
			String partName,
			String description,
			DamageSeverity severity,
			double confidence) {

		static Finding from(ClaimAnalysisFinding finding) {
			return new Finding(
					finding.partName(),
					finding.description(),
					finding.severity(),
					finding.confidence());
		}
	}
}
