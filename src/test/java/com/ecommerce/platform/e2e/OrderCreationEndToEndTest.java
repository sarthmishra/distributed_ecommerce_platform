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
import com.ecommerce.platform.security.dto.AuthResponse;
import com.ecommerce.platform.security.dto.LoginRequest;
import com.ecommerce.platform.security.dto.RegisterRequest;
import com.ecommerce.platform.security.service.AuthService;
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

    @Autowired
    private AuthService authService;

    private static final String PRODUCT_SKU = "LAPTOP-E2E-01";
    private static final String CUSTOMER_EMAIL = "priya@example.com";

    private Account customerAccount;
    private String authToken;

    @BeforeEach
    void setUp() {
        inventoryService.createProduct(new CreateProductRequest(
                PRODUCT_SKU, "E2E Test Laptop", "Used for saga e2e test", new BigDecimal("2000.00"), 5
        ));

        Account systemAccount = ledgerService.createAccount("system@settlement.com", AccountType.SYSTEM_SETTLEMENT);
        ledgerService.createAccount("merchant@store.com", AccountType.MERCHANT_REVENUE);

        AuthResponse authResponse = authService.register(new RegisterRequest(CUSTOMER_EMAIL, "password123"));
        this.authToken = authResponse.token();

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
        String url = "http://localhost:" + port + "/api/v1/orders";
        CreateOrderRequest request = new CreateOrderRequest(CUSTOMER_EMAIL, PRODUCT_SKU, 2);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(authToken);
        HttpEntity<CreateOrderRequest> httpEntity = new HttpEntity<>(request, headers);

        ResponseEntity<ApiResponse> response = restTemplate.postForEntity(url, httpEntity, ApiResponse.class);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().success());

        String orderNumber = (String) ((java.util.Map<?, ?>) response.getBody().data()).get("orderNumber");
        assertNotNull(orderNumber, "orderNumber should be present in the response");

        await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() ->
                        assertTrue(
                                outboxEventRepository.findAll().stream()
                                        .anyMatch(e -> e.getAggregateId().equals(orderNumber) && e.getEventType().equals("ORDER_CREATED")),
                                "Expected an ORDER_CREATED outbox row for " + orderNumber
                        )
                );

        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    OrderResponse order = orderService.getOrderByNumber(orderNumber);
                    assertEquals(OrderStatus.COMPLETED, order.status());
                });

        BigDecimal finalBalance = ledgerService.calculateAccountBalance(customerAccount);
        assertEquals(new BigDecimal("6000.00"), finalBalance, "10000 - (2 x 2000) = 6000");

        int remainingStock = inventoryService.getProductBySku(PRODUCT_SKU).stockQuantity();
        assertEquals(3, remainingStock, "5 - 2 reserved = 3 remaining");
    }
}