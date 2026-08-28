package com.steelsnake.cardamage.auth;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.reactive.server.WebTestClient;

import com.steelsnake.cardamage.claim.AdminClaimController;
import com.steelsnake.cardamage.claim.AdminClaimSummary;
import com.steelsnake.cardamage.claim.ClaimController;
import com.steelsnake.cardamage.claim.ClaimService;
import com.steelsnake.cardamage.claim.ClaimStatus;
import com.steelsnake.cardamage.claim.ClaimStatusResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@WebFluxTest(controllers = {AuthController.class, ClaimController.class, AdminClaimController.class})
@Import({SecurityConfig.class, AuthService.class, JwtService.class})
@TestPropertySource(properties = {
		"app.security.jwt.secret=test-jwt-secret-that-is-at-least-32-bytes",
		"app.security.jwt.expiration=PT1H"
})
class SecurityWebTests {

	private static final String EMAIL = "admin@test.local";
	private static final String PASSWORD = "correct-test-password";
	private static final String PASSWORD_HASH = new BCryptPasswordEncoder().encode(PASSWORD);

	@Autowired
	private WebTestClient webTestClient;
	@MockitoSpyBean
	private PasswordEncoder passwordEncoder;
	@Autowired
	private JwtEncoder jwtEncoder;

	@MockitoBean
	private AdminUserRepository adminUserRepository;
	@MockitoBean
	private ClaimService claimService;

	private AdminUser admin;

	@BeforeEach
	void configureAdmin() {
		this.admin = new AdminUser(
				UUID.randomUUID(), EMAIL, PASSWORD_HASH, Instant.now());
		when(this.adminUserRepository.findByEmail(EMAIL)).thenReturn(Mono.just(this.admin));
	}

	@Test
	void validCredentialsReturnJwt() {
		this.webTestClient.post()
				.uri("/api/auth/login")
				.bodyValue(new LoginRequest(EMAIL, PASSWORD))
				.exchange()
				.expectStatus().isOk()
				.expectBody()
				.jsonPath("$.accessToken").value(value -> assertThat(value).isInstanceOf(String.class))
				.jsonPath("$.tokenType").isEqualTo("Bearer")
				.jsonPath("$.expiresIn").isEqualTo(3600);
		verify(this.passwordEncoder, times(1)).matches(PASSWORD, this.admin.passwordHash());
	}

	@Test
	void knownEmailWithWrongPasswordReturnsUnauthorized() {
		this.webTestClient.post()
				.uri("/api/auth/login")
				.bodyValue(new LoginRequest(EMAIL, "wrong-password"))
				.exchange()
				.expectStatus().isUnauthorized()
				.expectBody().isEmpty();
		verify(this.passwordEncoder, times(1)).matches("wrong-password", this.admin.passwordHash());
	}

	@Test
	void unknownEmailStillRunsCostTenBcryptCheck() {
		String unknownEmail = "unknown@test.local";
		when(this.adminUserRepository.findByEmail(unknownEmail)).thenReturn(Mono.empty());

		this.webTestClient.post()
				.uri("/api/auth/login")
				.bodyValue(new LoginRequest(unknownEmail, PASSWORD))
				.exchange()
				.expectStatus().isUnauthorized()
				.expectBody().isEmpty();
		var hash = ArgumentCaptor.forClass(String.class);
		verify(this.passwordEncoder, times(1)).matches(eq(PASSWORD), hash.capture());
		assertThat(hash.getValue()).matches("\\$2[aby]\\$10\\$[./0-9A-Za-z]{53}");
	}

	@Test
	void publicClaimStatusDoesNotRequireToken() {
		UUID claimId = UUID.randomUUID();
		when(this.claimService.getStatus(claimId))
				.thenReturn(Mono.just(new ClaimStatusResponse(claimId, ClaimStatus.ANALYSIS_PENDING)));

		this.webTestClient.get()
				.uri("/api/claims/{id}/status", claimId)
				.exchange()
				.expectStatus().isOk()
				.expectBody()
				.jsonPath("$.id").isEqualTo(claimId.toString())
				.jsonPath("$.status").isEqualTo("ANALYSIS_PENDING");
	}

	@Test
	void adminEndpointWithoutTokenReturnsUnauthorized() {
		this.webTestClient.get()
				.uri("/api/admin/claims")
				.exchange()
				.expectStatus().isUnauthorized()
				.expectHeader().valueEquals(HttpHeaders.WWW_AUTHENTICATE, "Bearer")
				.expectBody()
				.jsonPath("$.status").isEqualTo(401);
	}

	@Test
	void malformedJwtReturnsUnauthorized() {
		this.webTestClient.get()
				.uri("/api/admin/claims")
				.headers(headers -> headers.setBearerAuth("not-a-jwt"))
				.exchange()
				.expectStatus().isUnauthorized()
				.expectBody()
				.jsonPath("$.status").isEqualTo(401);
	}

	@Test
	void expiredJwtReturnsUnauthorized() {
		Instant now = Instant.now();
		String token = token(
				this.jwtEncoder, JwtService.ISSUER,
				now.minusSeconds(7200), now.minusSeconds(3600), true);

		expectUnauthorized(token);
	}

	@Test
	void jwtWithoutExpirationReturnsUnauthorized() {
		Instant now = Instant.now();
		expectUnauthorized(token(this.jwtEncoder, JwtService.ISSUER, now, null, true));
	}

	@Test
	void jwtSignedWithDifferentSecretReturnsUnauthorized() {
		byte[] keyBytes = "different-test-jwt-secret-that-is-at-least-32-bytes"
				.getBytes(StandardCharsets.UTF_8);
		SecretKey key = new SecretKeySpec(keyBytes, "HmacSHA256");
		JwtEncoder otherEncoder = NimbusJwtEncoder.withSecretKey(key)
				.algorithm(MacAlgorithm.HS256)
				.build();
		Instant now = Instant.now();

		expectUnauthorized(token(
				otherEncoder, JwtService.ISSUER, now, now.plusSeconds(3600), true));
	}

	@Test
	void jwtWithWrongIssuerReturnsUnauthorized() {
		Instant now = Instant.now();

		expectUnauthorized(token(this.jwtEncoder, "another-issuer", now, now.plusSeconds(3600), true));
	}

	@Test
	void malformedAuthorizationHeaderReturnsUnauthorized() {
		this.webTestClient.get()
				.uri("/api/admin/claims")
				.header(HttpHeaders.AUTHORIZATION, "Bearer")
				.exchange()
				.expectStatus().isUnauthorized()
				.expectBody()
				.jsonPath("$.status").isEqualTo(401);
	}

	@Test
	void validAdminJwtGrantsAccess() {
		AdminClaimSummary claim = new AdminClaimSummary(
				UUID.randomUUID(), "Toyota", "Camry", 2022, ClaimStatus.ANALYSIS_PENDING, Instant.now());
		when(this.claimService.getAdminClaims()).thenReturn(Flux.just(claim));

		this.webTestClient.get()
				.uri("/api/admin/claims")
				.headers(headers -> headers.setBearerAuth(loginToken()))
				.exchange()
				.expectStatus().isOk()
				.expectBody()
				.jsonPath("$[0].id").isEqualTo(claim.id().toString())
				.jsonPath("$[0].carBrand").isEqualTo("Toyota")
				.jsonPath("$[0].passwordHash").doesNotExist();
	}

	@Test
	void validJwtWithoutAdminRoleReturnsForbidden() {
		this.webTestClient.get()
				.uri("/api/admin/claims")
				.headers(headers -> headers.setBearerAuth(tokenWithoutRoles()))
				.exchange()
				.expectStatus().isForbidden()
				.expectBody()
				.jsonPath("$.status").isEqualTo(403);
	}

	private String loginToken() {
		LoginResponse response = this.webTestClient.post()
				.uri("/api/auth/login")
				.bodyValue(new LoginRequest(EMAIL, PASSWORD))
				.exchange()
				.expectStatus().isOk()
				.expectBody(LoginResponse.class)
				.returnResult()
				.getResponseBody();
		assertThat(response).isNotNull();
		return response.accessToken();
	}

	private String tokenWithoutRoles() {
		Instant now = Instant.now();
		return token(this.jwtEncoder, JwtService.ISSUER, now, now.plusSeconds(3600), false);
	}

	private void expectUnauthorized(String token) {
		this.webTestClient.get()
				.uri("/api/admin/claims")
				.headers(headers -> headers.setBearerAuth(token))
				.exchange()
				.expectStatus().isUnauthorized()
				.expectBody()
				.jsonPath("$.status").isEqualTo(401);
	}

	private static String token(
			JwtEncoder encoder,
			String issuer,
			Instant issuedAt,
			Instant expiresAt,
			boolean includeAdminRole) {
		JwtClaimsSet.Builder claims = JwtClaimsSet.builder()
				.issuer(issuer)
				.subject(EMAIL)
				.issuedAt(issuedAt);
		if (expiresAt != null) {
			claims.expiresAt(expiresAt);
		}
		if (includeAdminRole) {
			claims.claim("roles", List.of("ADMIN"));
		}
		JwsHeader header = JwsHeader.with(MacAlgorithm.HS256).type("JWT").build();
		return encoder.encode(JwtEncoderParameters.from(header, claims.build())).getTokenValue();
	}
}
