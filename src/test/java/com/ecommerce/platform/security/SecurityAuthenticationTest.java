package com.ecommerce.platform.security;

import com.ecommerce.platform.common.dto.ApiResponse;
import com.ecommerce.platform.common.filter.CorrelationIdFilter;
import com.ecommerce.platform.inventory.dto.CreateProductRequest;
import com.ecommerce.platform.inventory.repository.ProductRepository;
import com.ecommerce.platform.inventory.service.InventoryService;
import com.ecommerce.platform.order.dto.CreateOrderRequest;
import com.ecommerce.platform.order.dto.OrderResponse;
import com.ecommerce.platform.order.service.OrderService;
import com.ecommerce.platform.payment.model.Account;
import com.ecommerce.platform.payment.model.AccountType;
import com.ecommerce.platform.payment.service.LedgerService;
import com.ecommerce.platform.security.dto.AuthResponse;
import com.ecommerce.platform.security.dto.LoginRequest;
import com.ecommerce.platform.security.dto.RegisterRequest;
import com.ecommerce.platform.security.model.Role;
import com.ecommerce.platform.security.model.User;
import com.ecommerce.platform.security.repository.UserRepository;
import com.ecommerce.platform.security.service.AuthService;
import com.ecommerce.platform.security.service.JwtTokenProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = "spring.datasource.url=jdbc:h2:mem:securitytestdb;DB_CLOSE_DELAY=-1")
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9098", "port=9098" })
public class SecurityAuthenticationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AuthService authService;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private PasswordEncoder passwordEncoder;

    @Autowired
    private JwtTokenProvider jwtTokenProvider;

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private LedgerService ledgerService;

    @Autowired
    private OrderService orderService;

    private static final String PRODUCT_SKU = "SEC-LAPTOP-01";
    private String customerToken;
    private String customer2Token;
    private String adminToken;

    @BeforeEach
    void setUp() {
        userRepository.deleteAll();

        if (productRepository.findBySku(PRODUCT_SKU).isEmpty()) {
            inventoryService.createProduct(new CreateProductRequest(
                    PRODUCT_SKU, "Security Laptop", "Test Laptop", new BigDecimal("1500.00"), 10
            ));
        }

        // Register customer 1
        AuthResponse customerAuth = authService.register(new RegisterRequest("customer1@example.com", "password123"));
        customerToken = customerAuth.token();

        // Register customer 2
        AuthResponse customer2Auth = authService.register(new RegisterRequest("customer2@example.com", "password123"));
        customer2Token = customer2Auth.token();

        // Create admin user manually
        User adminUser = new User(null, "admin@store.com", passwordEncoder.encode("admin123"), Role.ROLE_ADMIN);
        userRepository.save(adminUser);
        adminToken = jwtTokenProvider.generateToken(adminUser.getEmail(), adminUser.getRole().name());

        // Fund accounts
        Account systemAccount = ledgerService.createAccount("system@settlement.com", AccountType.SYSTEM_SETTLEMENT);
        ledgerService.createAccount("merchant@store.com", AccountType.MERCHANT_REVENUE);
        Account customer1Account = ledgerService.createAccount("customer1@example.com", AccountType.CUSTOMER_WALLET);
        Account customer2Account = ledgerService.createAccount("customer2@example.com", AccountType.CUSTOMER_WALLET);

        String depositId1 = "INIT-DEPOSIT-C1-" + UUID.randomUUID();
        String depositId2 = "INIT-DEPOSIT-C2-" + UUID.randomUUID();
        ledgerService.recordTransfer(depositId1, systemAccount.getAccountNumber(), customer1Account.getAccountNumber(), new BigDecimal("10000.00"), "Deposit");
        ledgerService.recordTransfer(depositId2, systemAccount.getAccountNumber(), customer2Account.getAccountNumber(), new BigDecimal("10000.00"), "Deposit");
    }

    @Test
    @DisplayName("Security: 1. Successful User Registration")
    void testSuccessfulRegistration() {
        RegisterRequest request = new RegisterRequest("newuser@example.com", "secret123");
        String url = "http://localhost:" + port + "/api/v1/auth/register";

        ResponseEntity<ApiResponse> response = restTemplate.postForEntity(url, request, ApiResponse.class);
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertTrue(response.getBody().success());

        User user = userRepository.findByEmail("newuser@example.com").orElse(null);
        assertNotNull(user);
        assertEquals(Role.ROLE_CUSTOMER, user.getRole());
    }

    @Test
    @DisplayName("Security: 2. Duplicate Registration Rejection")
    void testDuplicateRegistrationRejection() {
        RegisterRequest request = new RegisterRequest("customer1@example.com", "password123");
        String url = "http://localhost:" + port + "/api/v1/auth/register";

        ResponseEntity<ApiResponse> response = restTemplate.postForEntity(url, request, ApiResponse.class);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    }

    @Test
    @DisplayName("Security: 3. BCrypt Password Hashing (Password not in plaintext)")
    void testPasswordHashedWithBCrypt() {
        User user = userRepository.findByEmail("customer1@example.com").orElseThrow();
        assertNotEquals("password123", user.getPassword());
        assertTrue(passwordEncoder.matches("password123", user.getPassword()));
    }

    @Test
    @DisplayName("Security: 4. Successful Login")
    void testSuccessfulLogin() {
        LoginRequest request = new LoginRequest("customer1@example.com", "password123");
        String url = "http://localhost:" + port + "/api/v1/auth/login";

        ResponseEntity<ApiResponse> response = restTemplate.postForEntity(url, request, ApiResponse.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody().data());

        String token = (String) ((Map<?, ?>) response.getBody().data()).get("token");
        assertNotNull(token);
        assertTrue(jwtTokenProvider.validateToken(token));
        assertEquals("customer1@example.com", jwtTokenProvider.getEmailFromToken(token));
    }

    @Test
    @DisplayName("Security: 5. Invalid Password Login Rejection")
    void testInvalidPasswordLogin() {
        LoginRequest request = new LoginRequest("customer1@example.com", "wrongpassword");
        String url = "http://localhost:" + port + "/api/v1/auth/login";

        ResponseEntity<ApiResponse> response = restTemplate.postForEntity(url, request, ApiResponse.class);
        assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());
    }

    @Test
    @DisplayName("Security: 6. Missing JWT Rejection (401 Unauthorized)")
    void testMissingJwtRejection() {
        String url = "http://localhost:" + port + "/api/v1/orders";
        CreateOrderRequest request = new CreateOrderRequest("customer1@example.com", PRODUCT_SKU, 1);

        ResponseEntity<ApiResponse> response = restTemplate.postForEntity(url, request, ApiResponse.class);
        assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());
    }

    @Test
    @DisplayName("Security: 7. Invalid JWT Rejection (401 Unauthorized)")
    void testInvalidJwtRejection() {
        String url = "http://localhost:" + port + "/api/v1/orders";
        CreateOrderRequest request = new CreateOrderRequest("customer1@example.com", PRODUCT_SKU, 1);

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth("INVALID-JWT-TOKEN-STRING");
        HttpEntity<CreateOrderRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<ApiResponse> response = restTemplate.postForEntity(url, entity, ApiResponse.class);
        assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());
    }

    @Test
    @DisplayName("Security: 8. Expired JWT Rejection (401 Unauthorized)")
    void testExpiredJwtRejection() {
        JwtTokenProvider shortLivedProvider = new JwtTokenProvider(
                "404E635266556A586E3272357538782F413F4428472B4B6250645367566B5970", -1000L
        );
        String expiredToken = shortLivedProvider.generateToken("customer1@example.com", "ROLE_CUSTOMER");

        String url = "http://localhost:" + port + "/api/v1/orders";
        CreateOrderRequest request = new CreateOrderRequest("customer1@example.com", PRODUCT_SKU, 1);

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(expiredToken);
        HttpEntity<CreateOrderRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<ApiResponse> response = restTemplate.postForEntity(url, entity, ApiResponse.class);
        assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());
    }

    @Test
    @DisplayName("Security: 9. Customer Cannot Access Another Customer's Order (403 Forbidden)")
    void testCustomerCannotAccessOtherCustomerOrder() {
        OrderResponse order1 = orderService.createOrder(new CreateOrderRequest("customer1@example.com", PRODUCT_SKU, 1));

        String url = "http://localhost:" + port + "/api/v1/orders/" + order1.orderNumber();

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(customer2Token);
        HttpEntity<Void> entity = new HttpEntity<>(headers);

        ResponseEntity<ApiResponse> response = restTemplate.exchange(url, HttpMethod.GET, entity, ApiResponse.class);
        assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
    }

    @Test
    @DisplayName("Security: 10. Customer Can Access Their Own Order")
    void testCustomerCanAccessOwnOrder() {
        OrderResponse order1 = orderService.createOrder(new CreateOrderRequest("customer1@example.com", PRODUCT_SKU, 1));

        String url = "http://localhost:" + port + "/api/v1/orders/" + order1.orderNumber();

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(customerToken);
        HttpEntity<Void> entity = new HttpEntity<>(headers);

        ResponseEntity<ApiResponse> response = restTemplate.exchange(url, HttpMethod.GET, entity, ApiResponse.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertTrue(response.getBody().success());
    }

    @Test
    @DisplayName("Security: 11. Customer Cannot Access Another Customer's Account Balance (403 Forbidden)")
    void testCustomerCannotAccessOtherCustomerBalance() {
        Account c1Account = ledgerService.createAccount("customer1@example.com", AccountType.CUSTOMER_WALLET);

        String url = "http://localhost:" + port + "/api/v1/payments/accounts/" + c1Account.getAccountNumber() + "/balance";

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(customer2Token);
        HttpEntity<Void> entity = new HttpEntity<>(headers);

        ResponseEntity<ApiResponse> response = restTemplate.exchange(url, HttpMethod.GET, entity, ApiResponse.class);
        assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
    }

    @Test
    @DisplayName("Security: 12. Admin Access to Administrative Endpoint (POST /api/v1/products)")
    void testAdminAccessToAdminEndpoint() {
        String url = "http://localhost:" + port + "/api/v1/products";
        CreateProductRequest request = new CreateProductRequest("ADMIN-SKU-999", "Admin Product", "Desc", new BigDecimal("100.00"), 10);

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(adminToken);
        HttpEntity<CreateProductRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<ApiResponse> response = restTemplate.postForEntity(url, entity, ApiResponse.class);
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
    }

    @Test
    @DisplayName("Security: 13. Customer Rejection on Admin Endpoint (POST /api/v1/products -> 403 Forbidden)")
    void testCustomerRejectionOnAdminEndpoint() {
        String url = "http://localhost:" + port + "/api/v1/products";
        CreateProductRequest request = new CreateProductRequest("CUST-SKU-888", "Cust Product", "Desc", new BigDecimal("100.00"), 10);

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(customerToken);
        HttpEntity<CreateProductRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<ApiResponse> response = restTemplate.postForEntity(url, entity, ApiResponse.class);
        assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
    }

    @Test
    @DisplayName("Security: 14. Health Endpoint Accessibility (Public)")
    void testHealthEndpointPublic() {
        String url = "http://localhost:" + port + "/actuator/health";
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

    @Test
    @DisplayName("Security: 15. Metrics Endpoint Protection (Requires Admin/Auth)")
    void testMetricsEndpointProtection() {
        String url = "http://localhost:" + port + "/actuator/metrics";
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
        assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());
    }

    @Test
    @DisplayName("Security: 16. Correlation ID Preserved Alongside JWT Authentication")
    void testCorrelationIdPreservedWithJwt() {
        String expectedCorrelationId = "SEC-CORRELATION-ID-5555";
        String url = "http://localhost:" + port + "/api/v1/orders";
        CreateOrderRequest request = new CreateOrderRequest("customer1@example.com", PRODUCT_SKU, 1);

        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(customerToken);
        headers.set(CorrelationIdFilter.CORRELATION_ID_HEADER, expectedCorrelationId);
        HttpEntity<CreateOrderRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<ApiResponse> response = restTemplate.postForEntity(url, entity, ApiResponse.class);
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(expectedCorrelationId, response.getHeaders().getFirst(CorrelationIdFilter.CORRELATION_ID_HEADER));
    }
}
