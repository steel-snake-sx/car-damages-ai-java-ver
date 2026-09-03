package com.steelsnake.cardamage.claim;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClient;

import tools.jackson.databind.ObjectMapper;

// mock остаётся значением по умолчанию, чтобы приложение поднималось без ключа OpenAI
@Configuration
@ConditionalOnProperty(name = "app.ai.provider", havingValue = "openai")
class OpenAiAnalyzerConfig {

	@Bean
	DamageAnalyzer openAiDamageAnalyzer(
			ObjectMapper objectMapper,
			@Value("${app.ai.openai.base-url}") String baseUrl,
			@Value("${app.ai.openai.api-key:}") String apiKey,
			@Value("${app.ai.openai.model}") String model,
			@Value("${app.ai.openai.timeout}") Duration timeout) {
		if (apiKey.isBlank()) {
			throw new IllegalStateException(
					"app.ai.openai.api-key is required when app.ai.provider=openai");
		}
		WebClient webClient = WebClient.builder()
				.baseUrl(baseUrl)
				.defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey)
				.build();
		return new OpenAiDamageAnalyzer(webClient, objectMapper, model, timeout);
	}
}
