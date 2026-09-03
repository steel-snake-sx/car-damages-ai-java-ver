package com.steelsnake.cardamage.claim;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.http.client.reactive.MockClientHttpRequest;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class OpenAiDamageAnalyzerTests {

	private static final ObjectMapper objectMapper = JsonMapper.builder().build();
	private static final List<AnalysisImage> IMAGES =
			List.of(new AnalysisImage("image/png", new byte[] {1, 2, 3}));

	private final List<String> requestBodies = new ArrayList<>();

	@Test
	void structuredOutputIsParsedIntoValidatedAnalysis() {
		DamageAnalyzer analyzer = analyzer(HttpStatus.OK, completedResponse("""
				{
				  "car_detected": true,
				  "summary": "Повреждена передняя часть",
				  "confidence": 0.9,
				  "damages": [
				    {
				      "part_name": "Передний бампер",
				      "description": "Царапины",
				      "severity": "MEDIUM",
				      "confidence": 0.87
				    }
				  ]
				}"""));

		StepVerifier.create(analyzer.analyze(IMAGES))
				.assertNext(analysis -> {
					assertThat(analysis.carDetected()).isTrue();
					assertThat(analysis.summary()).isEqualTo("Повреждена передняя часть");
					assertThat(analysis.findings()).containsExactly(new DamageFinding(
							"Передний бампер", "Царапины", DamageSeverity.MEDIUM, 0.87));
				})
				.expectComplete()
				.verify(Duration.ofSeconds(5));
	}

	@Test
	void requestUsesStrictJsonSchemaAndSendsImagesAsDataUrls() {
		DamageAnalyzer analyzer = analyzer(HttpStatus.OK, completedResponse("""
				{"car_detected": false, "summary": "Автомобиль не распознан", \
				"confidence": 0.7, "damages": []}"""));

		StepVerifier.create(analyzer.analyze(IMAGES))
				.expectNextCount(1)
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		assertThat(this.requestBodies).hasSize(1);
		JsonNode request = objectMapper.readTree(this.requestBodies.getFirst());
		assertThat(request.path("store").isBoolean()).isTrue();
		assertThat(request.path("store").asBoolean()).isFalse();
		JsonNode format = request.path("text").path("format");
		assertThat(format.path("type").asString()).isEqualTo("json_schema");
		assertThat(format.path("strict").asBoolean()).isTrue();
		assertThat(format.path("schema").path("additionalProperties").asBoolean()).isFalse();
		JsonNode content = request.path("input").path(0).path("content");
		assertThat(content.path(0).path("type").asString()).isEqualTo("input_text");
		assertThat(content.path(1).path("image_url").asString())
				.isEqualTo("data:image/png;base64,AQID");
	}

	@Test
	void requestTimeoutIsReportedAsRetryable() {
		DamageAnalyzer analyzer = analyzer(HttpStatus.REQUEST_TIMEOUT, "request timed out");

		expectFailure(analyzer, AnalysisFailureReason.AI_UNAVAILABLE);
	}

	@Test
	void conflictIsReportedAsRetryable() {
		DamageAnalyzer analyzer = analyzer(HttpStatus.CONFLICT, "conflict");

		expectFailure(analyzer, AnalysisFailureReason.AI_UNAVAILABLE);
	}

	@Test
	void serverErrorIsReportedAsRetryable() {
		DamageAnalyzer analyzer = analyzer(HttpStatus.BAD_GATEWAY, "upstream failure");

		expectFailure(analyzer, AnalysisFailureReason.AI_UNAVAILABLE);
	}

	@Test
	void failedResponseWithServerErrorIsReportedAsRetryable() {
		DamageAnalyzer analyzer = analyzer(HttpStatus.OK, """
				{"status": "failed", "error": {
				"code": "server_error", "message": "The model failed to generate a response."
				}, "output": []} """);

		expectFailure(analyzer, AnalysisFailureReason.AI_UNAVAILABLE);
	}

	@Test
	void tooManyRequestsIsReportedAsRetryable() {
		DamageAnalyzer analyzer = analyzer(HttpStatus.TOO_MANY_REQUESTS, "slow down");

		expectFailure(analyzer, AnalysisFailureReason.AI_UNAVAILABLE);
	}

	@Test
	void hardQuotaIsNotRetried() {
		DamageAnalyzer analyzer = analyzer(HttpStatus.TOO_MANY_REQUESTS,
				"{\"error\":{\"code\":\"insufficient_quota\",\"message\":\"quota exceeded\"}}");

		expectFailure(analyzer, AnalysisFailureReason.AI_REQUEST_REJECTED);
	}

	@Test
	void unauthorizedIsNotRetryable() {
		DamageAnalyzer analyzer = analyzer(HttpStatus.UNAUTHORIZED, "invalid key");

		expectFailure(analyzer, AnalysisFailureReason.AI_REQUEST_REJECTED);
	}

	@Test
	void refusalIsNotRetryable() {
		DamageAnalyzer analyzer = analyzer(HttpStatus.OK, """
				{"status": "completed", "output": [{"type": "message", "content": [
				{"type": "refusal", "refusal": "Не могу помочь"}]}]}""");

		expectFailure(analyzer, AnalysisFailureReason.AI_REQUEST_REJECTED);
	}

	@Test
	void truncatedResponseIsReportedAsInvalidResult() {
		DamageAnalyzer analyzer = analyzer(HttpStatus.OK, """
				{"status": "incomplete", "incomplete_details": {"reason": "max_output_tokens"},
				"output": [{"type": "message", "content": [{"type": "output_text", "text": "{"}]}]}""");

		expectFailure(analyzer, AnalysisFailureReason.INVALID_AI_RESULT);
	}

	@Test
	void structuredOutputViolatingApplicationRulesIsRejected() {
		DamageAnalyzer analyzer = analyzer(HttpStatus.OK, completedResponse("""
				{"car_detected": true, "summary": "  ", "confidence": 0.9, "damages": []}"""));

		expectFailure(analyzer, AnalysisFailureReason.INVALID_AI_RESULT);
	}

	@Test
	void unexpectedSeverityValueIsRejected() {
		DamageAnalyzer analyzer = analyzer(HttpStatus.OK, completedResponse("""
				{"car_detected": true, "summary": "Повреждения есть", "confidence": 0.9, "damages": [
				{"part_name": "Бампер", "description": "Царапины", "severity": "CRITICAL",
				"confidence": 0.8}]}"""));

		expectFailure(analyzer, AnalysisFailureReason.INVALID_AI_RESULT);
	}

	@Test
	void transportFailureIsReportedAsRetryable() {
		WebClient webClient = WebClient.builder()
				.baseUrl("https://api.openai.test/v1")
				.exchangeFunction(request -> Mono.error(new java.io.IOException("connection reset")))
				.build();
		DamageAnalyzer analyzer = new OpenAiDamageAnalyzer(
				webClient, objectMapper, "gpt-test", Duration.ofSeconds(5));

		expectFailure(analyzer, AnalysisFailureReason.AI_UNAVAILABLE);
	}

	private static void expectFailure(DamageAnalyzer analyzer, AnalysisFailureReason reason) {
		StepVerifier.create(analyzer.analyze(IMAGES))
				.expectErrorSatisfies(error -> {
					assertThat(error).isInstanceOf(DamageAnalysisException.class);
					assertThat(((DamageAnalysisException) error).reason()).isEqualTo(reason);
				})
				.verify(Duration.ofSeconds(5));
	}

	private static String completedResponse(String structuredOutput) {
		return objectMapper.writeValueAsString(java.util.Map.of(
				"status", "completed",
				"output", List.of(java.util.Map.of(
						"type", "message",
						"content", List.of(java.util.Map.of(
								"type", "output_text",
								"text", structuredOutput))))));
	}

	private DamageAnalyzer analyzer(HttpStatus status, String responseBody) {
		WebClient webClient = WebClient.builder()
				.baseUrl("https://api.openai.test/v1")
				.exchangeFunction(request -> recordRequest(request)
						.thenReturn(ClientResponse.create(status)
								.header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
								.body(responseBody)
								.build()))
				.build();
		return new OpenAiDamageAnalyzer(webClient, objectMapper, "gpt-test", Duration.ofSeconds(5));
	}

	private Mono<Void> recordRequest(ClientRequest request) {
		assertThat(request.url().getPath()).isEqualTo("/v1/responses");
		MockClientHttpRequest recorded = new MockClientHttpRequest(HttpMethod.POST, request.url());
		return request.writeTo(recorded, ExchangeStrategies.withDefaults())
				.then(Mono.defer(recorded::getBodyAsString))
				.doOnNext(this.requestBodies::add)
				.then();
	}
}
