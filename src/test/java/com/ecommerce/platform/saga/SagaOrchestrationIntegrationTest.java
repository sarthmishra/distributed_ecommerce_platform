package com.ecommerce.platform.saga;

import com.ecommerce.platform.event.config.KafkaTopicConfig;
import com.ecommerce.platform.event.dto.OrderCreatedEvent;
import com.ecommerce.platform.event.dto.PaymentCompletedEvent;
import com.ecommerce.platform.inventory.dto.CreateProductRequest;
import com.ecommerce.platform.inventory.service.InventoryService;
import com.ecommerce.platform.order.dto.CreateOrderRequest;
import com.ecommerce.platform.order.dto.OrderResponse;
import com.ecommerce.platform.order.model.OrderStatus;
import com.ecommerce.platform.order.service.OrderService;
import com.ecommerce.platform.payment.model.Account;
import com.ecommerce.platform.payment.model.AccountType;
import com.ecommerce.platform.payment.service.LedgerService;
import com.ecommerce.platform.saga.service.SagaOrchestrator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9094", "port=9094" })
public class SagaOrchestrationIntegrationTest {

    @Autowired
    private SagaOrchestrator sagaOrchestrator;

    @Autowired
    private OrderService orderService;

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private LedgerService ledgerService;

    private Account customerAccount;
    private Account merchantAccount;
    private Account systemAccount;
    private String productSku;

    @BeforeEach
    void setUp() {
        productSku = "PHONE-15-PRO";
        inventoryService.createProduct(new CreateProductRequest(
                productSku, "Phone 15 Pro", "Flagship phone", new BigDecimal("1000.00"), 5
        ));

        systemAccount = ledgerService.createAccount("system@settlement.com", AccountType.SYSTEM_SETTLEMENT);
        customerAccount = ledgerService.createAccount("bob@example.com", AccountType.CUSTOMER_WALLET);
        merchantAccount = ledgerService.createAccount("merchant@store.com", AccountType.MERCHANT_REVENUE);

        // Initial deposit ₹5000.00 into Bob's wallet
        ledgerService.recordTransfer(
                "INIT-DEPOSIT-BOB",
                systemAccount.getAccountNumber(),
                customerAccount.getAccountNumber(),
                new BigDecimal("5000.00"),
                "Wallet funding"
        );
    }

    @Test
    @DisplayName("Should execute end-to-end Saga Happy Path: Order -> Payment -> Inventory -> COMPLETED")
    void testSagaHappyPath() {
        // Step 1: Create Order
        OrderResponse order = orderService.createOrder(new CreateOrderRequest("bob@example.com", productSku, 2));
        assertEquals(OrderStatus.PENDING, order.status());

        // Step 2: Trigger Saga Step 1 (OrderCreatedEvent -> Payment)
        OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent(
                order.orderNumber(), order.customerEmail(), order.productSku(), order.quantity(), order.totalAmount()
        );
        sagaOrchestrator.handleOrderCreated(orderCreatedEvent);

        // Verify Payment Completed
        OrderResponse updatedOrder = orderService.getOrderByNumber(order.orderNumber());
        assertEquals(OrderStatus.PAYMENT_COMPLETED, updatedOrder.status());
        assertEquals(new BigDecimal("3000.00"), ledgerService.calculateAccountBalance(customerAccount));

        // Step 3: Trigger Saga Step 2 (PaymentCompletedEvent -> Stock Reservation)
        PaymentCompletedEvent paymentCompletedEvent = new PaymentCompletedEvent(
                "TXN-TEST-SAGA", order.orderNumber(), order.customerEmail(), order.totalAmount()
        );
        sagaOrchestrator.handlePaymentCompleted(paymentCompletedEvent);

        // Verify Order Completed and Stock Depleted
        OrderResponse finalOrder = orderService.getOrderByNumber(order.orderNumber());
        assertEquals(OrderStatus.COMPLETED, finalOrder.status());
        assertEquals(3, inventoryService.getProductBySku(productSku).stockQuantity());
    }

    @Test
    @DisplayName("Should execute Saga Compensating Refund when stock reservation fails")
    void testSagaCompensatingRefund() {
        // Create an order for 10 items (when only 5 are in stock)
        OrderResponse order = orderService.createOrder(new CreateOrderRequest("bob@example.com", productSku, 10));

        // Trigger Saga Step 1 (Payment succeeds first)
        OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent(
                order.orderNumber(), order.customerEmail(), order.productSku(), order.quantity(), order.totalAmount()
        );
        sagaOrchestrator.handleOrderCreated(orderCreatedEvent);

        // Customer wallet debited ₹10,000.00 -> balance drops from 5000 to -5000 (or payment processed)
        // Trigger Saga Step 2 (Stock reservation fails -> triggers compensating refund)
        PaymentCompletedEvent paymentCompletedEvent = new PaymentCompletedEvent(
                "TXN-TEST-SAGA-FAIL", order.orderNumber(), order.customerEmail(), order.totalAmount()
        );
        sagaOrchestrator.handlePaymentCompleted(paymentCompletedEvent);

        // Verify Order is CANCELLED and Compensating Refund restored customer balance
        OrderResponse finalOrder = orderService.getOrderByNumber(order.orderNumber());
        assertEquals(OrderStatus.CANCELLED, finalOrder.status());
        assertEquals(new BigDecimal("5000.00"), ledgerService.calculateAccountBalance(customerAccount));
    }
}
