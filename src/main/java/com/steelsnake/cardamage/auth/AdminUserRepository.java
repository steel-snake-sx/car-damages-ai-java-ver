package com.steelsnake.cardamage.auth;

import java.util.UUID;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import reactor.core.publisher.Mono;

public interface AdminUserRepository extends ReactiveCrudRepository<AdminUser, UUID> {

	Mono<AdminUser> findByEmail(String email);
}
