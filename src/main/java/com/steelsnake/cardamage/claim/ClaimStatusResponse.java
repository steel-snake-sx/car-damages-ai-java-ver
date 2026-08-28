package com.steelsnake.cardamage.claim;

import java.util.UUID;

public record ClaimStatusResponse(UUID id, ClaimStatus status) {
}
