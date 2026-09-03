package com.steelsnake.cardamage.claim;

import java.util.UUID;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import io.swagger.v3.oas.annotations.security.SecurityRequirement;

@RestController
@RequestMapping("/api/admin/claims")
@SecurityRequirement(name = "bearerAuth")
public class AdminClaimController {

	private final ClaimService claimService;

	AdminClaimController(ClaimService claimService) {
		this.claimService = claimService;
	}

	@GetMapping
	public Flux<AdminClaimSummary> getClaims() {
		return this.claimService.getAdminClaims();
	}

	@GetMapping("/{id}")
	public Mono<AdminClaimDetails> getClaim(@PathVariable UUID id) {
		return this.claimService.getAdminClaim(id);
	}
}
