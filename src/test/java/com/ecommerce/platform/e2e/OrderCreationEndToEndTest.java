package com.ecommerce.platform.e2e;

import com.ecommerce.platform.common.dto.ApiResponse;
import com.ecommerce.platform.inventory.dto.CreateProductRequest;
import com.ecommerce.platform.inventory.service.InventoryService;
import com.ecommerce.platform.order.dto.CreateOrderRequest;
import com.ecommerce.platform.order.dto.OrderResponse;
import com.ecommerce.platform.order.model.OrderStatus;
import com.ecommerce.platform.order.service.OrderService;
import com.ecommerce.platform.outbox.repository.OutboxEventRepository;
import com.ecommerce.platform.payment.model.Account;
import com.ecommerce.platform.payment.model.AccountType;
import com.ecommerce.platform.payment.service.LedgerService;
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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

// class: OrderCreationEndToEndTest
// does: proves the REAL production path works end-to-end -
//       POST /api/v1/orders (real HTTP call, real controller)
//         -> OrderService writes an OrderCreatedEvent row to the outbox table (same DB transaction)
//         -> OutboxPublisherService's @Scheduled poller picks it up and publishes to Kafka
//         -> SagaOrchestrator consumes it, debits the wallet, publishes PaymentCompletedEvent
//         -> SagaOrchestrator consumes that, reserves stock, marks the order COMPLETED
// Unlike the older SagaOrchestrationIntegrationTest, this test never calls sagaOrchestrator
// methods directly - it only talks to the REST API, so it actually catches wiring bugs
// like "OrderService never publishes anything" (which is what was originally broken).
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9095", "port=9095" })
public class OrderCreationEndToEndTest {

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

    private static final String PRODUCT_SKU = "LAPTOP-E2E-01";
    private static final String CUSTOMER_EMAIL = "priya@example.com";

    private Account customerAccount;

    @BeforeEach
    void setUp() {
        // seed a product with 5 units in stock
        inventoryService.createProduct(new CreateProductRequest(
                PRODUCT_SKU, "E2E Test Laptop", "Used for saga e2e test", new BigDecimal("2000.00"), 5
        ));

        // seed the merchant account and a funded customer wallet
        Account systemAccount = ledgerService.createAccount("system@settlement.com", AccountType.SYSTEM_SETTLEMENT);
        ledgerService.createAccount("merchant@store.com", AccountType.MERCHANT_REVENUE);
        customerAccount = ledgerService.createAccount(CUSTOMER_EMAIL, AccountType.CUSTOMER_WALLET);

        ledgerService.recordTransfer(
                "INIT-DEPOSIT-PRIYA",
                systemAccount.getAccountNumber(),
                customerAccount.getAccountNumber(),
                new BigDecimal("10000.00"),
                "Wallet funding for e2e test"
        );
    }

    @Test
    @DisplayName("POST /api/v1/orders should flow through outbox -> Kafka -> saga to COMPLETED, with no direct saga calls")
    void orderCreatedViaRestApi_flowsThroughOutboxAndSaga_toCompletion() {
        // Step 1: hit the real REST endpoint, exactly like a real client would
        String url = "http://localhost:" + port + "/api/v1/orders";
        CreateOrderRequest request = new CreateOrderRequest(CUSTOMER_EMAIL, PRODUCT_SKU, 2);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<CreateOrderRequest> httpEntity = new HttpEntity<>(request, headers);

        ResponseEntity<ApiResponse> response = restTemplate.postForEntity(url, httpEntity, ApiResponse.class);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().success());

        String orderNumber = (String) ((java.util.Map<?, ?>) response.getBody().data()).get("orderNumber");
        assertNotNull(orderNumber, "orderNumber should be present in the response");

        // Step 2: confirm an outbox row was actually written for this order
        // (this is the exact thing that was previously never happening)
        await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() ->
                        assertTrue(
                                outboxEventRepository.findAll().stream()
                                        .anyMatch(e -> e.getAggregateId().equals(orderNumber) && e.getEventType().equals("ORDER_CREATED")),
                                "Expected an ORDER_CREATED outbox row for " + orderNumber
                        )
                );

        // Step 3: wait for the outbox poller (runs every 2s) to publish to Kafka and for the
        // saga to run both steps, WITHOUT ever calling sagaOrchestrator or eventPublisher ourselves
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    OrderResponse order = orderService.getOrderByNumber(orderNumber);
                    assertEquals(OrderStatus.COMPLETED, order.status());
                });

        // Step 4: verify the side effects actually happened - wallet debited, stock reduced
        BigDecimal finalBalance = ledgerService.calculateAccountBalance(customerAccount);
        assertEquals(new BigDecimal("6000.00"), finalBalance, "10000 - (2 x 2000) = 6000");

        int remainingStock = inventoryService.getProductBySku(PRODUCT_SKU).stockQuantity();
        assertEquals(3, remainingStock, "5 - 2 reserved = 3 remaining");
    }
}