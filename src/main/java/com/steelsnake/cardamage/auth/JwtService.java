package com.steelsnake.cardamage.auth;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.stereotype.Service;

@Service
public class JwtService {

	static final String ISSUER = "car-damage-api";

	private final JwtEncoder jwtEncoder;
	private final Duration expiration;

	JwtService(JwtEncoder jwtEncoder, @Value("${app.security.jwt.expiration}") Duration expiration) {
		this.jwtEncoder = jwtEncoder;
		this.expiration = expiration;
	}

	LoginResponse createToken(AdminUser admin) {
		Instant issuedAt = Instant.now();
		JwtClaimsSet claims = JwtClaimsSet.builder()
				.issuer(ISSUER)
				.subject(admin.email())
				.issuedAt(issuedAt)
				.expiresAt(issuedAt.plus(this.expiration))
				.claim("roles", List.of("ADMIN"))
				.build();
		JwsHeader header = JwsHeader.with(MacAlgorithm.HS256).type("JWT").build();
		String token = this.jwtEncoder.encode(JwtEncoderParameters.from(header, claims)).getTokenValue();
		return new LoginResponse(token, "Bearer", this.expiration.toSeconds());
	}
}
