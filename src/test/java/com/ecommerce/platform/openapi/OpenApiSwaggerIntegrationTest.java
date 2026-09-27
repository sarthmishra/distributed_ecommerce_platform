package com.ecommerce.platform.openapi;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9099", "port=9099" })
public class OpenApiSwaggerIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    @DisplayName("OpenAPI: 1. Public Access to /v3/api-docs JSON spec")
    void testOpenApiDocsJsonAccessible() {
        String url = "http://localhost:" + port + "/v3/api-docs";
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().contains("Distributed E-Commerce Platform API"));
        assertTrue(response.getBody().contains("BearerAuth"));
    }

    @Test
    @DisplayName("OpenAPI: 2. Public Access to /swagger-ui/index.html")
    void testSwaggerUiHtmlAccessible() {
        String url = "http://localhost:" + port + "/swagger-ui/index.html";
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().contains("Swagger UI"));
    }

    @Test
    @DisplayName("OpenAPI: 3. Protected API Endpoint remains protected (401 Unauthorized without token)")
    void testProtectedEndpointStillRequiresAuth() {
        String url = "http://localhost:" + port + "/api/v1/orders/ORD-123456";
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);

        assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());
    }
}
