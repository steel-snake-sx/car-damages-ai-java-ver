package com.steelsnake.cardamage.claim;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DamageAnalysisTests {

	@Test
	void mockAnalyzerReturnsTheSameResultForEveryRequest() {
		MockDamageAnalyzer analyzer = new MockDamageAnalyzer();
		List<AnalysisImage> images = List.of(new AnalysisImage("image/png", new byte[] {1}));

		DamageAnalysis first = analyzer.analyze(images).block(Duration.ofSeconds(1));
		DamageAnalysis second = analyzer.analyze(List.of()).block(Duration.ofSeconds(1));

		assertThat(first).isEqualTo(second);
		assertThat(first).isNotNull();
		assertThat(first.carDetected()).isTrue();
		assertThat(first.findings()).hasSize(2);
		StepVerifier.create(analyzer.analyze(images))
				.expectNext(first)
				.expectComplete()
				.verify(Duration.ofSeconds(1));
	}

	@Test
	void blankSummaryIsRejected() {
		assertThatThrownBy(() -> new DamageAnalysis(true, "   ", 0.5, List.of()))
				.isInstanceOf(DamageAnalysisException.class)
				.hasMessageContaining("summary");
	}

	@Test
	void confidenceOutsideZeroToOneIsRejected() {
		assertThatThrownBy(() -> new DamageAnalysis(true, "Повреждений нет", 1.5, List.of()))
				.isInstanceOf(DamageAnalysisException.class)
				.hasMessageContaining("confidence");
		assertThatThrownBy(() -> new DamageFinding("Бампер", "Царапина", DamageSeverity.LOW, -0.1))
				.isInstanceOf(DamageAnalysisException.class);
	}

	@Test
	void findingsWithoutADetectedCarAreRejected() {
		assertThatThrownBy(() -> new DamageAnalysis(
				false,
				"Автомобиль не распознан",
				0.9,
				List.of(new DamageFinding("Бампер", "Царапина", DamageSeverity.LOW, 0.5))))
				.isInstanceOf(DamageAnalysisException.class)
				.hasMessageContaining("findings must be empty");
	}

	@Test
	void tooManyFindingsAreRejected() {
		List<DamageFinding> findings = java.util.stream.IntStream.rangeClosed(
						0, DamageAnalysis.MAX_FINDINGS)
				.mapToObj(index -> new DamageFinding(
						"Деталь " + index, "Повреждение", DamageSeverity.LOW, 0.5))
				.toList();

		assertThatThrownBy(() -> new DamageAnalysis(true, "Много повреждений", 0.9, findings))
				.isInstanceOf(DamageAnalysisException.class)
				.hasMessageContaining("findings must not exceed");
	}

	@Test
	void unknownSeverityIsRejected() {
		assertThat(DamageSeverity.from("high")).isEqualTo(DamageSeverity.HIGH);
		assertThatThrownBy(() -> DamageSeverity.from("critical"))
				.isInstanceOf(DamageAnalysisException.class)
				.hasMessageContaining("severity");
	}

	@Test
	void oversizedTextIsRejected() {
		String longSummary = "а".repeat(DamageAnalysis.MAX_SUMMARY_LENGTH + 1);

		assertThatThrownBy(() -> new DamageAnalysis(true, longSummary, 0.9, List.of()))
				.isInstanceOf(DamageAnalysisException.class)
				.hasMessageContaining("must not exceed");
	}

	@Test
	void controlCharactersAreStrippedFromModelText() {
		DamageFinding finding = new DamageFinding(
				" Передний\u0000бампер ", "Царапины", DamageSeverity.LOW, 0.5);

		assertThat(finding.partName()).isEqualTo("Передний бампер");
	}
}
