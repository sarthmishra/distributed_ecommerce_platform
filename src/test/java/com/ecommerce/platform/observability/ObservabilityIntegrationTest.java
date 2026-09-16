package com.ecommerce.platform.observability;

import com.ecommerce.platform.common.dto.ApiResponse;
import com.ecommerce.platform.common.filter.CorrelationIdFilter;
import com.ecommerce.platform.inventory.dto.CreateProductRequest;
import com.ecommerce.platform.inventory.service.InventoryService;
import com.ecommerce.platform.order.dto.CreateOrderRequest;
import com.ecommerce.platform.order.dto.OrderResponse;
import com.ecommerce.platform.order.model.OrderStatus;
import com.ecommerce.platform.order.service.OrderService;
import com.ecommerce.platform.outbox.model.OutboxEvent;
import com.ecommerce.platform.outbox.repository.OutboxEventRepository;
import com.ecommerce.platform.payment.model.Account;
import com.ecommerce.platform.payment.model.AccountType;
import com.ecommerce.platform.payment.service.LedgerService;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9097", "port=9097" })
public class ObservabilityIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private LedgerService ledgerService;

    @Autowired
    private OrderService orderService;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private MeterRegistry meterRegistry;

    private static final String PRODUCT_SKU = "OBS-PHONE-01";
    private static final String CUSTOMER_EMAIL = "obs_user@example.com";

    @BeforeEach
    void setUp() {
        inventoryService.createProduct(new CreateProductRequest(
                PRODUCT_SKU, "Observability Phone", "Test Phone", new BigDecimal("1200.00"), 10
        ));

        Account systemAccount = ledgerService.createAccount("system@settlement.com", AccountType.SYSTEM_SETTLEMENT);
        ledgerService.createAccount("merchant@store.com", AccountType.MERCHANT_REVENUE);
        Account customerAccount = ledgerService.createAccount(CUSTOMER_EMAIL, AccountType.CUSTOMER_WALLET);

        ledgerService.recordTransfer(
                "INIT-DEPOSIT-OBS",
                systemAccount.getAccountNumber(),
                customerAccount.getAccountNumber(),
                new BigDecimal("10000.00"),
                "Wallet funding for observability test"
        );
    }

    @Test
    @DisplayName("End-to-End Observability: X-Correlation-ID is preserved in HTTP response, Outbox, and metrics are incremented")
    void testEndToEndCorrelationIdAndMetricsPropagation() {
        String expectedCorrelationId = "OBS-CORRELATION-ID-7777";
        String url = "http://localhost:" + port + "/api/v1/orders";
        CreateOrderRequest request = new CreateOrderRequest(CUSTOMER_EMAIL, PRODUCT_SKU, 2);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(CorrelationIdFilter.CORRELATION_ID_HEADER, expectedCorrelationId);
        HttpEntity<CreateOrderRequest> httpEntity = new HttpEntity<>(request, headers);

        ResponseEntity<ApiResponse> response = restTemplate.postForEntity(url, httpEntity, ApiResponse.class);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getHeaders().getFirst(CorrelationIdFilter.CORRELATION_ID_HEADER));
        assertEquals(expectedCorrelationId, response.getHeaders().getFirst(CorrelationIdFilter.CORRELATION_ID_HEADER));

        String orderNumber = (String) ((Map<?, ?>) response.getBody().data()).get("orderNumber");
        assertNotNull(orderNumber);

        // Verify correlation ID persisted in OutboxEvent row
        await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    Optional<OutboxEvent> outboxOpt = outboxEventRepository.findAll().stream()
                            .filter(e -> e.getAggregateId().equals(orderNumber) && e.getEventType().equals("ORDER_CREATED"))
                            .findFirst();
                    assertTrue(outboxOpt.isPresent(), "OutboxEvent row should exist");
                    assertEquals(expectedCorrelationId, outboxOpt.get().getCorrelationId(), "OutboxEvent should persist correlation ID");
                });

        // Await until Saga completes order flow to COMPLETED
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    OrderResponse order = orderService.getOrderByNumber(orderNumber);
                    assertEquals(OrderStatus.COMPLETED, order.status());
                });

        // Verify Micrometer metrics counters
        double ordersCreated = meterRegistry.find("orders.created").counter() != null
                ? meterRegistry.find("orders.created").counter().count() : 0;
        double outboxProcessed = meterRegistry.find("outbox.processed").counter() != null
                ? meterRegistry.find("outbox.processed").counter().count() : 0;
        double ordersCompleted = meterRegistry.find("orders.completed").counter() != null
                ? meterRegistry.find("orders.completed").counter().count() : 0;

        assertTrue(ordersCreated > 0, "orders.created metric counter should be incremented");
        assertTrue(outboxProcessed > 0, "outbox.processed metric counter should be incremented");
        assertTrue(ordersCompleted > 0, "orders.completed metric counter should be incremented");
    }
}
