package com.steelsnake.cardamage;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityScheme;

@Configuration
class OpenApiConfig {

	@Bean
	OpenAPI carDamageOpenAPI() {
		return new OpenAPI()
				.info(new Info()
						.title("Car Damage AI API")
						.description("Backend for asynchronous vehicle damage analysis using Kafka and AI")
						.version("1.0"))
				.components(new Components()
						.addSecuritySchemes("bearerAuth", new SecurityScheme()
								.type(SecurityScheme.Type.HTTP)
								.scheme("bearer")
								.bearerFormat("JWT")));
	}
}
