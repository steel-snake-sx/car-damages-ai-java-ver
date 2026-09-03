package com.steelsnake.cardamage.claim;

// содержимое читаем в память только на время запроса к AI: изображений максимум три и они уже ограничены по размеру
public record AnalysisImage(String contentType, byte[] content) {
}
