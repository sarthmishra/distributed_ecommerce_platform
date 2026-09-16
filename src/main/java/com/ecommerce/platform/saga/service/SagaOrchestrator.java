package com.ecommerce.platform.saga.service;

import com.ecommerce.platform.event.config.KafkaTopicConfig;
import com.ecommerce.platform.event.dto.*;
import com.ecommerce.platform.event.publisher.EventPublisher;
import com.ecommerce.platform.inventory.service.InventoryService;
import com.ecommerce.platform.order.dto.OrderResponse;
import com.ecommerce.platform.order.model.OrderStatus;
import com.ecommerce.platform.order.service.OrderService;
import com.ecommerce.platform.payment.model.Account;
import com.ecommerce.platform.payment.model.AccountType;
import com.ecommerce.platform.payment.repository.AccountRepository;
import com.ecommerce.platform.payment.service.LedgerService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.util.UUID;

@Service
public class SagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);
    private static final String MERCHANT_EMAIL = "merchant@store.com";

    private final OrderService orderService;
    private final InventoryService inventoryService;
    private final LedgerService ledgerService;
    private final AccountRepository accountRepository;
    private final EventPublisher eventPublisher;

    private final Counter ordersCompletedCounter;
    private final Counter ordersFailedCounter;
    private final Counter paymentSuccessCounter;
    private final Counter paymentFailedCounter;
    private final Counter inventoryFailedCounter;

    public SagaOrchestrator(OrderService orderService,
                            InventoryService inventoryService,
                            LedgerService ledgerService,
                            AccountRepository accountRepository,
                            EventPublisher eventPublisher,
                            MeterRegistry meterRegistry) {
        this.orderService = orderService;
        this.inventoryService = inventoryService;
        this.ledgerService = ledgerService;
        this.accountRepository = accountRepository;
        this.eventPublisher = eventPublisher;

        this.ordersCompletedCounter = Counter.builder("orders.completed")
                .description("Total number of orders successfully completed")
                .register(meterRegistry);
        this.ordersFailedCounter = Counter.builder("orders.failed")
                .description("Total number of failed or cancelled orders")
                .register(meterRegistry);
        this.paymentSuccessCounter = Counter.builder("payment.success")
                .description("Total number of successful payment operations")
                .register(meterRegistry);
        this.paymentFailedCounter = Counter.builder("payment.failed")
                .description("Total number of failed payment operations")
                .register(meterRegistry);
        this.inventoryFailedCounter = Counter.builder("inventory.failed")
                .description("Total number of inventory reservation failures")
                .register(meterRegistry);
    }

    private Account getOrCreateAccount(String email, AccountType type) {
        return accountRepository.findByUserEmailAndAccountType(email, type)
                .orElseGet(() -> ledgerService.createAccount(email, type));
    }

    public void handleOrderCreated(OrderCreatedEvent event) {
        handleOrderCreated(event, null);
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_ORDERS_CREATED, groupId = "saga-group")
    @Transactional
    public void handleOrderCreated(OrderCreatedEvent event,
                                   @Header(name = "X-Correlation-ID", required = false) String correlationId) {
        setupMdc(correlationId, event.orderNumber(), "PAYMENT");
        try {
            log.info("[Saga Step 1] Processing OrderCreatedEvent for Order {}", event.orderNumber());

            OrderResponse currentOrder = orderService.getOrderByNumber(event.orderNumber());
            if (currentOrder.status() != OrderStatus.PENDING) {
                log.warn("[Saga Idempotency] Order {} already in status {}, skipping duplicate OrderCreatedEvent",
                        event.orderNumber(), currentOrder.status());
                return;
            }

            try {
                Account customerAccount = getOrCreateAccount(event.customerEmail(), AccountType.CUSTOMER_WALLET);
                Account merchantAccount = getOrCreateAccount(MERCHANT_EMAIL, AccountType.MERCHANT_REVENUE);

                String txnId = "TXN-SAGA-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

                ledgerService.recordTransfer(
                        txnId,
                        customerAccount.getAccountNumber(),
                        merchantAccount.getAccountNumber(),
                        event.totalAmount(),
                        "Saga Payment for Order " + event.orderNumber()
                );

                orderService.updateOrderStatus(event.orderNumber(), OrderStatus.PAYMENT_COMPLETED);
                paymentSuccessCounter.increment();

                PaymentCompletedEvent paymentEvent = new PaymentCompletedEvent(
                        txnId, event.orderNumber(), event.customerEmail(), event.totalAmount()
                );
                eventPublisher.publishEvent(KafkaTopicConfig.TOPIC_PAYMENTS_COMPLETED, event.orderNumber(), paymentEvent, correlationId);

            } catch (Exception ex) {
                paymentFailedCounter.increment();
                ordersFailedCounter.increment();

                log.error("[Saga Failure] Payment step failed for Order {}: {}", event.orderNumber(), ex.getMessage());
                orderService.updateOrderStatus(event.orderNumber(), OrderStatus.PAYMENT_FAILED);

                PaymentFailedEvent failedEvent = new PaymentFailedEvent(event.orderNumber(), event.customerEmail(), ex.getMessage());
                eventPublisher.publishEvent(KafkaTopicConfig.TOPIC_PAYMENTS_FAILED, event.orderNumber(), failedEvent, correlationId);
            }
        } finally {
            clearMdc();
        }
    }

    public void handlePaymentCompleted(PaymentCompletedEvent event) {
        handlePaymentCompleted(event, null);
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_PAYMENTS_COMPLETED, groupId = "saga-group")
    @Transactional
    public void handlePaymentCompleted(PaymentCompletedEvent event,
                                      @Header(name = "X-Correlation-ID", required = false) String correlationId) {
        setupMdc(correlationId, event.orderNumber(), "INVENTORY");
        try {
            log.info("[Saga Step 2] Processing PaymentCompletedEvent for Order {}", event.orderNumber());

            OrderResponse order = orderService.getOrderByNumber(event.orderNumber());

            if (order.status() != OrderStatus.PAYMENT_COMPLETED) {
                log.warn("[Saga Idempotency] Order {} already in status {}, skipping duplicate PaymentCompletedEvent",
                        event.orderNumber(), order.status());
                return;
            }

            try {
                inventoryService.reserveStock(order.productSku(), order.quantity());
                orderService.updateOrderStatus(event.orderNumber(), OrderStatus.COMPLETED);
                ordersCompletedCounter.increment();

                InventoryReservedEvent reservedEvent = new InventoryReservedEvent(event.orderNumber(), order.productSku(), order.quantity());
                eventPublisher.publishEvent(KafkaTopicConfig.TOPIC_INVENTORY_RESERVED, event.orderNumber(), reservedEvent, correlationId);

                log.info("[Saga SUCCESS] Order {} completed end-to-end!", event.orderNumber());

            } catch (Exception ex) {
                inventoryFailedCounter.increment();
                log.error("[Saga Failure] Stock reservation failed for Order {}. Triggering Compensating Refund Transaction!", event.orderNumber());

                InventoryFailedEvent failedEvent = new InventoryFailedEvent(event.orderNumber(), order.productSku(), ex.getMessage());
                eventPublisher.publishEvent(KafkaTopicConfig.TOPIC_INVENTORY_FAILED, event.orderNumber(), failedEvent, correlationId);
            }
        } finally {
            clearMdc();
        }
    }

    public void handleInventoryFailedCompensateRefund(InventoryFailedEvent event) {
        handleInventoryFailedCompensateRefund(event, null);
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_INVENTORY_FAILED, groupId = "saga-group")
    @Transactional
    public void handleInventoryFailedCompensateRefund(InventoryFailedEvent event,
                                                       @Header(name = "X-Correlation-ID", required = false) String correlationId) {
        setupMdc(correlationId, event.orderNumber(), "COMPENSATING_REFUND");
        try {
            log.warn("[Saga Compensating Action] Refunding customer payment for cancelled Order {}", event.orderNumber());

            OrderResponse order = orderService.getOrderByNumber(event.orderNumber());

            if (order.status() == OrderStatus.CANCELLED) {
                log.warn("[Saga Idempotency] Order {} already CANCELLED, skipping duplicate compensating refund",
                        event.orderNumber());
                return;
            }

            try {
                Account customerAccount = getOrCreateAccount(order.customerEmail(), AccountType.CUSTOMER_WALLET);
                Account merchantAccount = getOrCreateAccount(MERCHANT_EMAIL, AccountType.MERCHANT_REVENUE);

                String refundTxnId = "REFUND-SAGA-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

                ledgerService.recordTransfer(
                        refundTxnId,
                        merchantAccount.getAccountNumber(),
                        customerAccount.getAccountNumber(),
                        order.totalAmount(),
                        "Saga Compensation: Refund for cancelled Order " + event.orderNumber()
                );

                orderService.updateOrderStatus(event.orderNumber(), OrderStatus.CANCELLED);
                ordersFailedCounter.increment();
                log.info("[Saga Compensation Complete] Refunded {} to customer for Order {}", order.totalAmount(), event.orderNumber());

            } catch (Exception ex) {
                log.error("[Saga Critical Alert] Failed to execute compensating refund for Order {}: {}", event.orderNumber(), ex.getMessage(), ex);
            }
        } finally {
            clearMdc();
        }
    }

    private void setupMdc(String correlationId, String orderNumber, String sagaStep) {
        if (StringUtils.hasText(correlationId)) {
            MDC.put("correlationId", correlationId);
        }
        if (StringUtils.hasText(orderNumber)) {
            MDC.put("orderNumber", orderNumber);
        }
        if (StringUtils.hasText(sagaStep)) {
            MDC.put("sagaStep", sagaStep);
        }
    }

    private void clearMdc() {
        MDC.remove("correlationId");
        MDC.remove("orderNumber");
        MDC.remove("sagaStep");
    }
}