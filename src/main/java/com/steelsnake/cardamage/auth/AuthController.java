package com.steelsnake.cardamage.auth;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

	private final AuthService authService;

	AuthController(AuthService authService) {
		this.authService = authService;
	}

	@PostMapping("/login")
	public Mono<ResponseEntity<LoginResponse>> login(@RequestBody LoginRequest request) {
		return this.authService.login(request.email(), request.password())
				.map(ResponseEntity::ok)
				.defaultIfEmpty(ResponseEntity.status(401).build());
	}
}
