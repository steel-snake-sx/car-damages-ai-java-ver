package com.steelsnake.cardamage.claim;

import java.util.List;

import reactor.core.publisher.Mono;

// реальная заменяемая граница: настоящий OpenAI и детерминированный mock
public interface DamageAnalyzer {

	Mono<DamageAnalysis> analyze(List<AnalysisImage> images);
}
