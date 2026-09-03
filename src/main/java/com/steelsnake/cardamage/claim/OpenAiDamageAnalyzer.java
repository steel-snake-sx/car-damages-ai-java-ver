package com.steelsnake.cardamage.claim;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

// один запрос к Responses API: изображения уходят только здесь, стоимость ремонта не запрашивается
class OpenAiDamageAnalyzer implements DamageAnalyzer {

	private static final String INSTRUCTIONS = """
			Ты автомобильный эксперт по оценке повреждений после ДТП.
			Определи по фотографиям, изображён ли автомобиль, и перечисли только те повреждения,
			которые действительно видны на снимках.
			Отвечай на русском языке: русские названия деталей и русские описания повреждений.
			Не придумывай повреждения без визуальных подтверждений и не оценивай стоимость ремонта.
			Если автомобиль не распознан, укажи car_detected=false и пустой список damages.
			Проверь зоны отдельно: передняя часть, задняя часть, левый и правый борт, крыша, стёкла, оптика.
			severity: LOW - косметическое повреждение, MEDIUM - требуется ремонт детали,
			HIGH - деталь под замену или повреждена силовая структура.
			confidence - уверенность от 0 до 1.""";

	private static final String PROMPT = "Проанализируй повреждения автомобиля на этих фотографиях";
	private static final int MAX_OUTPUT_TOKENS = 1_500;

	private final WebClient webClient;
	private final ObjectMapper objectMapper;
	private final String model;
	private final Duration timeout;

	OpenAiDamageAnalyzer(WebClient webClient, ObjectMapper objectMapper, String model, Duration timeout) {
		this.webClient = webClient;
		this.objectMapper = objectMapper;
		this.model = model;
		this.timeout = timeout;
	}

	@Override
	public Mono<DamageAnalysis> analyze(List<AnalysisImage> images) {
		// base64 и сериализация тела нагружают CPU на несколько мегабайт, поэтому держим их вне event loop
		return Mono.fromCallable(() -> requestBody(images))
				.subscribeOn(Schedulers.boundedElastic())
				.flatMap(this::send)
				.timeout(this.timeout)
				.onErrorMap(error -> !(error instanceof DamageAnalysisException),
						error -> DamageAnalysisException.unavailable("OpenAI request failed", error))
				.map(this::parseAnalysis);
	}

	private Mono<String> send(byte[] body) {
		return this.webClient.post()
				.uri("/responses")
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(body)
				.retrieve()
				.onStatus(HttpStatusCode::isError, response -> response.bodyToMono(String.class)
						.defaultIfEmpty("")
						.map(errorBody -> statusError(response.statusCode(), errorBody)))
				.bodyToMono(String.class)
				.switchIfEmpty(Mono.error(DamageAnalysisException.invalidResult("OpenAI response is empty")));
	}

	private DamageAnalysisException statusError(HttpStatusCode status, String body) {
		String message = "OpenAI responded with " + status.value() + ": " + shorten(body);
		if (status.value() == 429 && isHardQuota(body)) {
			return DamageAnalysisException.requestRejected(message, null);
		}
		// временные ответы можно повторить, а обычные 4xx повторять бессмысленно
		if (status.value() == 408 || status.value() == 409
				|| status.value() == 429 || status.is5xxServerError()) {
			return DamageAnalysisException.unavailable(message, null);
		}
		return DamageAnalysisException.requestRejected(message, null);
	}

	private boolean isHardQuota(String body) {
		try {
			JsonNode response = this.objectMapper.readTree(body);
			return response != null
					&& "insufficient_quota".equals(response.path("error").path("code").asString(""));
		}
		catch (JacksonException exception) {
			return false;
		}
	}

	private DamageAnalysis parseAnalysis(String body) {
		JsonNode response = readJson(body, "OpenAI response is not valid JSON");
		String status = response.path("status").asString("");
		if ("failed".equals(status)) {
			JsonNode error = response.path("error");
			String code = error.path("code").asString("unknown");
			String message = "OpenAI response failed with " + code + ": "
					+ shorten(error.path("message").asString(""));
			if ("server_error".equals(code)) {
				throw DamageAnalysisException.unavailable(message, null);
			}
			throw DamageAnalysisException.requestRejected(message, null);
		}
		if (!"completed".equals(status)) {
			// обрезанный или отфильтрованный ответ повторять с тем же входом смысла нет
			throw DamageAnalysisException.invalidResult(
					"OpenAI response status is " + status
							+ " (" + response.path("incomplete_details").path("reason").asString("unknown") + ")");
		}
		JsonNode message = firstMessage(response);
		JsonNode content = firstContent(message);
		if ("refusal".equals(content.path("type").asString(""))) {
			throw DamageAnalysisException.requestRejected(
					"OpenAI refused the request: " + shorten(content.path("refusal").asString("")), null);
		}

		JsonNode payload = readJson(
				content.path("text").asString(""), "OpenAI structured output is not valid JSON");
		List<DamageFinding> findings = new ArrayList<>();
		JsonNode damages = payload.path("damages");
		if (!damages.isArray()) {
			throw DamageAnalysisException.invalidResult("damages must be an array");
		}
		for (JsonNode damage : damages) {
			findings.add(new DamageFinding(
					nullableText(damage.path("part_name")),
					nullableText(damage.path("description")),
					DamageSeverity.from(nullableText(damage.path("severity"))),
					confidence(damage.path("confidence"))));
		}
		if (!payload.path("car_detected").isBoolean()) {
			throw DamageAnalysisException.invalidResult("car_detected must be a boolean");
		}
		return new DamageAnalysis(
				payload.path("car_detected").asBoolean(),
				nullableText(payload.path("summary")),
				confidence(payload.path("confidence")),
				findings);
	}

	private JsonNode readJson(String value, String failureMessage) {
		try {
			return this.objectMapper.readTree(value);
		}
		catch (JacksonException exception) {
			throw DamageAnalysisException.invalidResult(failureMessage);
		}
	}

	private static JsonNode firstMessage(JsonNode response) {
		for (JsonNode item : response.path("output")) {
			if ("message".equals(item.path("type").asString(""))) {
				return item;
			}
		}
		throw DamageAnalysisException.invalidResult("OpenAI response has no message output");
	}

	private static JsonNode firstContent(JsonNode message) {
		JsonNode content = message.path("content").path(0);
		if (content.isMissingNode()) {
			throw DamageAnalysisException.invalidResult("OpenAI message has no content");
		}
		return content;
	}

	private static String nullableText(JsonNode node) {
		return node.isString() ? node.stringValue() : null;
	}

	private static double confidence(JsonNode node) {
		if (!node.isNumber()) {
			throw DamageAnalysisException.invalidResult("confidence must be a number");
		}
		return node.asDouble();
	}

	private byte[] requestBody(List<AnalysisImage> images) {
		List<Map<String, Object>> content = new ArrayList<>();
		content.add(Map.of("type", "input_text", "text", PROMPT));
		Base64.Encoder encoder = Base64.getEncoder();
		for (AnalysisImage image : images) {
			content.add(Map.of(
					"type", "input_image",
					"image_url", "data:" + image.contentType() + ";base64,"
							+ encoder.encodeToString(image.content())));
		}

		Map<String, Object> body = new LinkedHashMap<>();
		body.put("model", this.model);
		body.put("store", false);
		body.put("instructions", INSTRUCTIONS);
		body.put("max_output_tokens", MAX_OUTPUT_TOKENS);
		body.put("input", List.of(Map.of("role", "user", "content", content)));
		body.put("text", Map.of("format", responseSchema()));
		return this.objectMapper.writeValueAsBytes(body);
	}

	// strict structured output требует перечислить все свойства в required и запретить дополнительные
	private static Map<String, Object> responseSchema() {
		Map<String, Object> damage = Map.of(
				"type", "object",
				"properties", Map.of(
						"part_name", Map.of("type", "string"),
						"description", Map.of("type", "string"),
						"severity", Map.of("type", "string", "enum", List.of("LOW", "MEDIUM", "HIGH")),
						"confidence", Map.of("type", "number")),
				"required", List.of("part_name", "description", "severity", "confidence"),
				"additionalProperties", false);
		Map<String, Object> schema = Map.of(
				"type", "object",
				"properties", Map.of(
						"car_detected", Map.of("type", "boolean"),
						"summary", Map.of("type", "string"),
						"confidence", Map.of("type", "number"),
						"damages", Map.of("type", "array", "items", damage)),
				"required", List.of("car_detected", "summary", "confidence", "damages"),
				"additionalProperties", false);
		return Map.of(
				"type", "json_schema",
				"name", "damage_analysis",
				"strict", true,
				"schema", schema);
	}

	private static String shorten(String value) {
		String normalized = value == null ? "" : value.replace('\n', ' ').strip();
		return normalized.length() > 200 ? normalized.substring(0, 200) : normalized;
	}
}
