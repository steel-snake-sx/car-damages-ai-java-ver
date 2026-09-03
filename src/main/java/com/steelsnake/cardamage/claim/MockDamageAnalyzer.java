package com.steelsnake.cardamage.claim;

import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import reactor.core.publisher.Mono;

// демонстрационный анализ без внешнего вызова: результат фиксированный, чтобы запуск без ключа был воспроизводим
@Component
@ConditionalOnProperty(name = "app.ai.provider", havingValue = "mock", matchIfMissing = true)
class MockDamageAnalyzer implements DamageAnalyzer {

	@Override
	public Mono<DamageAnalysis> analyze(List<AnalysisImage> images) {
		return Mono.fromSupplier(() -> new DamageAnalysis(
				true,
				"Обнаружены повреждения передней части автомобиля без признаков нарушения геометрии кузова",
				0.9,
				List.of(
						new DamageFinding(
								"Передний бампер",
								"Царапины и потёртости лакокрасочного покрытия",
								DamageSeverity.MEDIUM,
								0.91),
						new DamageFinding(
								"Левое переднее крыло",
								"Небольшая вмятина у колёсной арки",
								DamageSeverity.LOW,
								0.88))));
	}
}
