package com.steelsnake.cardamage.auth;

import java.util.Locale;
import java.util.Optional;

import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
public class AuthService {

	private static final String DUMMY_PASSWORD_HASH =
			"$2a$10$dXJ3SW6G7P50lGmMkkmwe.20cQQubK3.HZWzG3YB1tlRy.fqvM/BG";

	private final AdminUserRepository adminUserRepository;
	private final PasswordEncoder passwordEncoder;
	private final JwtService jwtService;

	AuthService(
			AdminUserRepository adminUserRepository,
			PasswordEncoder passwordEncoder,
			JwtService jwtService) {
		this.adminUserRepository = adminUserRepository;
		this.passwordEncoder = passwordEncoder;
		this.jwtService = jwtService;
	}

	Mono<LoginResponse> login(String email, String password) {
		if (email == null || email.isBlank() || password == null || password.isBlank()) {
			return Mono.empty();
		}

		return this.adminUserRepository.findByEmail(email.strip().toLowerCase(Locale.ROOT))
				.map(Optional::of)
				.defaultIfEmpty(Optional.empty())
				.flatMap(admin -> passwordMatches(
						password,
						admin.map(AdminUser::passwordHash).orElse(DUMMY_PASSWORD_HASH))
						.filter(matches -> matches && admin.isPresent())
						.map(matches -> this.jwtService.createToken(admin.orElseThrow())));
	}

	private Mono<Boolean> passwordMatches(String password, String passwordHash) {
		return Mono.fromCallable(() -> this.passwordEncoder.matches(password, passwordHash))
				.subscribeOn(Schedulers.boundedElastic());
	}
}
