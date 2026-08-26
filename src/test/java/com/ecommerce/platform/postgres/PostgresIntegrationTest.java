package com.ecommerce.platform.postgres;

import com.ecommerce.platform.inventory.dto.CreateProductRequest;
import com.ecommerce.platform.inventory.dto.ProductResponse;
import com.ecommerce.platform.inventory.repository.ProductRepository;
import com.ecommerce.platform.inventory.service.InventoryService;
import com.ecommerce.platform.order.dto.CreateOrderRequest;
import com.ecommerce.platform.order.dto.OrderResponse;
import com.ecommerce.platform.order.model.OrderStatus;
import com.ecommerce.platform.order.repository.OrderRepository;
import com.ecommerce.platform.order.service.OrderService;
import com.ecommerce.platform.outbox.model.OutboxEvent;
import com.ecommerce.platform.outbox.repository.OutboxEventRepository;
import com.ecommerce.platform.payment.model.Account;
import com.ecommerce.platform.payment.model.AccountType;
import com.ecommerce.platform.payment.model.IdempotentRecord;
import com.ecommerce.platform.payment.model.JournalEntry;
import com.ecommerce.platform.payment.repository.AccountRepository;
import com.ecommerce.platform.payment.repository.IdempotencyRepository;
import com.ecommerce.platform.payment.repository.JournalEntryRepository;
import com.ecommerce.platform.payment.service.IdempotencyService;
import com.ecommerce.platform.payment.service.LedgerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("postgres")
public class PostgresIntegrationTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private LedgerService ledgerService;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private JournalEntryRepository journalEntryRepository;

    @Autowired
    private IdempotencyService idempotencyService;

    @Autowired
    private IdempotencyRepository idempotencyRepository;

    @BeforeEach
    void cleanUpDatabase() {
        outboxEventRepository.deleteAll();
        orderRepository.deleteAll();
        journalEntryRepository.deleteAll();
        accountRepository.deleteAll();
        productRepository.deleteAll();
        idempotencyRepository.deleteAll();
    }

    @Test
    @DisplayName("PostgreSQL Profile: Connection, Schema, Order creation, and OutboxEvent persistence")
    void testPostgresOrderAndOutboxPersistence() {
        String sku = "PG-LAPTOP-" + UUID.randomUUID().toString().substring(0, 8);
        inventoryService.createProduct(new CreateProductRequest(sku, "PG Laptop", "Test Laptop", new BigDecimal("1500.00"), 10));

        CreateOrderRequest request = new CreateOrderRequest("pguser@example.com", sku, 2);
        OrderResponse orderResponse = orderService.createOrder(request);

        assertNotNull(orderResponse);
        assertNotNull(orderResponse.orderNumber());
        assertEquals(OrderStatus.PENDING, orderResponse.status());

        // Verify OutboxEvent row was persisted atomically in PostgreSQL
        List<OutboxEvent> outboxEvents = outboxEventRepository.findAll();
        Optional<OutboxEvent> matchingOutbox = outboxEvents.stream()
                .filter(e -> e.getAggregateId().equals(orderResponse.orderNumber()) && e.getEventType().equals("ORDER_CREATED"))
                .findFirst();

        assertTrue(matchingOutbox.isPresent(), "Expected an ORDER_CREATED outbox row in PostgreSQL table for order " + orderResponse.orderNumber());
        assertNotNull(matchingOutbox.get().getPayload(), "Outbox payload should be non-null text");
    }

    @Test
    @DisplayName("PostgreSQL Profile: Pessimistic inventory row locking (findBySkuWithLock) in PostgreSQL")
    void testPostgresPessimisticInventoryLocking() {
        String sku = "PG-PHONE-" + UUID.randomUUID().toString().substring(0, 8);
        inventoryService.createProduct(new CreateProductRequest(sku, "PG Phone", "Test Phone", new BigDecimal("800.00"), 8));

        ProductResponse reserved = inventoryService.reserveStock(sku, 3);
        assertEquals(5, reserved.stockQuantity());

        ProductResponse current = inventoryService.getProductBySku(sku);
        assertEquals(5, current.stockQuantity());
    }

    @Test
    @DisplayName("PostgreSQL Profile: Double-entry ledger transfers and balance calculations in PostgreSQL")
    void testPostgresLedgerAndBalance() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        Account system = ledgerService.createAccount("pgsystem-" + uid + "@bank.com", AccountType.SYSTEM_SETTLEMENT);
        Account customer = ledgerService.createAccount("pgcustomer-" + uid + "@example.com", AccountType.CUSTOMER_WALLET);
        Account merchant = ledgerService.createAccount("pgmerchant-" + uid + "@store.com", AccountType.MERCHANT_REVENUE);

        // Initial deposit
        ledgerService.recordTransfer("TXN-PG-INIT-" + uid, system.getAccountNumber(), customer.getAccountNumber(), new BigDecimal("5000.00"), "Deposit");
        assertEquals(new BigDecimal("5000.00"), ledgerService.calculateAccountBalance(customer));

        // Transfer to merchant
        JournalEntry journal = ledgerService.recordTransfer("TXN-PG-PAY-" + uid, customer.getAccountNumber(), merchant.getAccountNumber(), new BigDecimal("1200.00"), "Payment");
        assertNotNull(journal);
        assertEquals(2, journal.getEntries().size());

        assertEquals(new BigDecimal("3800.00"), ledgerService.calculateAccountBalance(customer));
        assertEquals(new BigDecimal("1200.00"), ledgerService.calculateAccountBalance(merchant));
    }

    @Test
    @DisplayName("PostgreSQL Profile: Idempotency record creation and TEXT response payload caching in PostgreSQL")
    void testPostgresIdempotencyPersistence() {
        String uid = UUID.randomUUID().toString().substring(0, 8);
        String key = "PG-IDEMPOTENCY-KEY-" + uid;
        String reqHash = "HASH-" + uid;
        String respBody = "{\"status\":\"SUCCESS\",\"txn\":\"TXN-" + uid + "\"}";

        IdempotentRecord record = idempotencyService.saveRecord(key, reqHash, 200, respBody);
        assertNotNull(record.getId());
        assertEquals(key, record.getIdempotencyKey());

        Optional<IdempotentRecord> fetched = idempotencyService.getRecord(key);
        assertTrue(fetched.isPresent());
        assertEquals(respBody, fetched.get().getResponseBody());
    }
}
