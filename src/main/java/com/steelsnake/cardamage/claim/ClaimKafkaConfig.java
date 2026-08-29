package com.steelsnake.cardamage.claim;

import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
class ClaimKafkaConfig {

	@Bean
	NewTopic damageAnalysisRequestedTopic() {
		return TopicBuilder.name(DamageAnalysisRequested.TOPIC)
				.partitions(1)
				.replicas(1)
				.build();
	}

	// ошибки обработки валидной записи остаются внутри Mono
	@Bean
	DefaultErrorHandler analysisRequestErrorHandler() {
		DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(0L, 0L));
		// при async ack не двигаем offset через seek
		errorHandler.setSeekAfterError(false);
		return errorHandler;
	}
}
