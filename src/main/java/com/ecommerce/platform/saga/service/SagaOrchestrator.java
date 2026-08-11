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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class SagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

    // constant: MERCHANT_EMAIL - single hardcoded merchant identity used across the saga
    private static final String MERCHANT_EMAIL = "merchant@store.com";

    private final OrderService orderService;
    private final InventoryService inventoryService;
    private final LedgerService ledgerService;
    private final AccountRepository accountRepository;
    private final EventPublisher eventPublisher;

    public SagaOrchestrator(OrderService orderService,
                            InventoryService inventoryService,
                            LedgerService ledgerService,
                            AccountRepository accountRepository,
                            EventPublisher eventPublisher) {
        this.orderService = orderService;
        this.inventoryService = inventoryService;
        this.ledgerService = ledgerService;
        this.accountRepository = accountRepository;
        this.eventPublisher = eventPublisher;
    }

    // method: getOrCreateAccount
    // does: finds a wallet/revenue account by its real owner (email + type) instead of
    //       mistakenly searching by accountNumber; creates one on first use
    private Account getOrCreateAccount(String email, AccountType type) {
        return accountRepository.findByUserEmailAndAccountType(email, type)
                .orElseGet(() -> ledgerService.createAccount(email, type));
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_ORDERS_CREATED, groupId = "saga-group")
    @Transactional
    public void handleOrderCreated(OrderCreatedEvent event) {
        log.info("[Saga Step 1] Processing OrderCreatedEvent for Order {}", event.orderNumber());

        // idempotency guard: if a redelivered Kafka message arrives for an order that
        // already moved past PENDING, skip re-processing instead of double-charging
        OrderResponse currentOrder = orderService.getOrderByNumber(event.orderNumber());
        if (currentOrder.status() != OrderStatus.PENDING) {
            log.warn("[Saga Idempotency] Order {} already in status {}, skipping duplicate OrderCreatedEvent",
                    event.orderNumber(), currentOrder.status());
            return;
        }

        try {
            // variable: customerAccount / merchantAccount - looked up by (email, type), not accountNumber
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

            PaymentCompletedEvent paymentEvent = new PaymentCompletedEvent(
                    txnId, event.orderNumber(), event.customerEmail(), event.totalAmount()
            );
            eventPublisher.publishEvent(KafkaTopicConfig.TOPIC_PAYMENTS_COMPLETED, event.orderNumber(), paymentEvent);

        } catch (Exception ex) {
            log.error("[Saga Failure] Payment step failed for Order {}: {}", event.orderNumber(), ex.getMessage());
            orderService.updateOrderStatus(event.orderNumber(), OrderStatus.PAYMENT_FAILED);

            PaymentFailedEvent failedEvent = new PaymentFailedEvent(event.orderNumber(), event.customerEmail(), ex.getMessage());
            eventPublisher.publishEvent(KafkaTopicConfig.TOPIC_PAYMENTS_FAILED, event.orderNumber(), failedEvent);
        }
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_PAYMENTS_COMPLETED, groupId = "saga-group")
    @Transactional
    public void handlePaymentCompleted(PaymentCompletedEvent event) {
        log.info("[Saga Step 2] Processing PaymentCompletedEvent for Order {}", event.orderNumber());

        OrderResponse order = orderService.getOrderByNumber(event.orderNumber());

        // idempotency guard: only reserve stock once, from the correct prior state
        if (order.status() != OrderStatus.PAYMENT_COMPLETED) {
            log.warn("[Saga Idempotency] Order {} already in status {}, skipping duplicate PaymentCompletedEvent",
                    event.orderNumber(), order.status());
            return;
        }

        try {
            inventoryService.reserveStock(order.productSku(), order.quantity());
            orderService.updateOrderStatus(event.orderNumber(), OrderStatus.COMPLETED);

            InventoryReservedEvent reservedEvent = new InventoryReservedEvent(event.orderNumber(), order.productSku(), order.quantity());
            eventPublisher.publishEvent(KafkaTopicConfig.TOPIC_INVENTORY_RESERVED, event.orderNumber(), reservedEvent);

            log.info("[Saga SUCCESS] Order {} completed end-to-end!", event.orderNumber());

        } catch (Exception ex) {
            log.error("[Saga Failure] Stock reservation failed for Order {}. Triggering Compensating Refund Transaction!", event.orderNumber());

            InventoryFailedEvent failedEvent = new InventoryFailedEvent(event.orderNumber(), order.productSku(), ex.getMessage());
            eventPublisher.publishEvent(KafkaTopicConfig.TOPIC_INVENTORY_FAILED, event.orderNumber(), failedEvent);
        }
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_INVENTORY_FAILED, groupId = "saga-group")
    @Transactional
    public void handleInventoryFailedCompensateRefund(InventoryFailedEvent event) {
        log.warn("[Saga Compensating Action] Refunding customer payment for cancelled Order {}", event.orderNumber());

        OrderResponse order = orderService.getOrderByNumber(event.orderNumber());

        // idempotency guard: don't refund twice for the same failed order
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
            log.info("[Saga Compensation Complete] Refunded {} to customer for Order {}", order.totalAmount(), event.orderNumber());

        } catch (Exception ex) {
            log.error("[Saga Critical Alert] Failed to execute compensating refund for Order {}: {}", event.orderNumber(), ex.getMessage(), ex);
        }
    }
}