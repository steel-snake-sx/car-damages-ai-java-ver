package com.steelsnake.cardamage.auth;

public record LoginResponse(String accessToken, String tokenType, long expiresIn) {
}
