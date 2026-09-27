package com.ecommerce.platform.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "JWT_EXPIRATION_MS=3600000",
                "SPRING_JPA_SHOW_SQL=true"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9100", "port=9100" })
public class ProductionConfigurationTest {

    @Value("${jwt.expiration}")
    private long jwtExpirationMs;

    @Value("${spring.jpa.show-sql}")
    private boolean showSql;

    @Test
    @DisplayName("Configuration: 1. JWT expiration can be overridden via environment variable placeholder")
    void testJwtExpirationEnvOverride() {
        assertEquals(3600000L, jwtExpirationMs);
    }

    @Test
    @DisplayName("Configuration: 2. JPA show-sql can be configured via environment variable placeholder")
    void testJpaShowSqlEnvOverride() {
        assertEquals(true, showSql);
    }
}
