package com.steelsnake.cardamage.claim;

import java.util.UUID;

public record DamageAnalysisRequested(int version, UUID claimId) {

	public static final String TOPIC = "damage-analysis.requested";
	public static final int VERSION = 1;

	static DamageAnalysisRequested of(UUID claimId) {
		return new DamageAnalysisRequested(VERSION, claimId);
	}
}
