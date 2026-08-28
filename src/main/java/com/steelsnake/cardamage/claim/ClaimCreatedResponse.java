package com.steelsnake.cardamage.claim;

import java.util.UUID;

public record ClaimCreatedResponse(UUID id, ClaimStatus status) {
}
