package com.ecommerce.platform.order.service;

import com.ecommerce.platform.common.exception.ResourceNotFoundException;
import com.ecommerce.platform.event.dto.OrderCreatedEvent;
import com.ecommerce.platform.inventory.dto.ProductResponse;
import com.ecommerce.platform.inventory.service.InventoryService;
import com.ecommerce.platform.order.dto.CreateOrderRequest;
import com.ecommerce.platform.order.dto.OrderResponse;
import com.ecommerce.platform.order.model.Order;
import com.ecommerce.platform.order.model.OrderStatus;
import com.ecommerce.platform.order.repository.OrderRepository;
import com.ecommerce.platform.outbox.service.OutboxPublisherService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final InventoryService inventoryService;
    // field: outboxPublisherService - writes the OrderCreatedEvent in the SAME db transaction as the order
    private final OutboxPublisherService outboxPublisherService;
    // field: objectMapper - serializes the event to JSON for outbox storage
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository,
                        InventoryService inventoryService,
                        OutboxPublisherService outboxPublisherService,
                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.inventoryService = inventoryService;
        this.outboxPublisherService = outboxPublisherService;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {
        ProductResponse product = inventoryService.getProductBySku(request.productSku());

        BigDecimal totalAmount = product.price().multiply(BigDecimal.valueOf(request.quantity()));
        String orderNumber = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        Order order = new Order(
                null,
                orderNumber,
                request.customerEmail(),
                request.productSku(),
                request.quantity(),
                totalAmount,
                OrderStatus.PENDING
        );

        Order savedOrder = orderRepository.save(order);
        log.info("Order created successfully: {} for customer: {}", savedOrder.getOrderNumber(), savedOrder.getCustomerEmail());

        // block: write OrderCreatedEvent to the outbox table in the SAME transaction as the order save.
        // This guarantees the event is never lost even if Kafka is down at this instant -
        // the scheduled OutboxPublisherService.processOutboxEvents() will pick it up and publish it.
        OrderCreatedEvent event = new OrderCreatedEvent(
                savedOrder.getOrderNumber(),
                savedOrder.getCustomerEmail(),
                savedOrder.getProductSku(),
                savedOrder.getQuantity(),
                savedOrder.getTotalAmount()
        );
        try {
            String payload = objectMapper.writeValueAsString(event);
            outboxPublisherService.saveToOutbox("ORDER", savedOrder.getOrderNumber(), "ORDER_CREATED", payload);
        } catch (JsonProcessingException ex) {
            // if this fails, the whole @Transactional method rolls back - order + outbox row
            // are saved atomically or not at all, which is the entire point of the outbox pattern
            throw new IllegalStateException("Failed to serialize OrderCreatedEvent for order " + orderNumber, ex);
        }

        return OrderResponse.fromEntity(savedOrder);
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrderByNumber(String orderNumber) {
        Order order = orderRepository.findByOrderNumber(orderNumber)
                .orElseThrow(() -> new ResourceNotFoundException("Order not found with number: " + orderNumber));
        return OrderResponse.fromEntity(order);
    }

    @Transactional
    public OrderResponse updateOrderStatus(String orderNumber, OrderStatus status) {
        Order order = orderRepository.findByOrderNumber(orderNumber)
                .orElseThrow(() -> new ResourceNotFoundException("Order not found with number: " + orderNumber));

        order.setStatus(status);
        Order updatedOrder = orderRepository.save(order);
        log.info("Updated order status for {}: {}", orderNumber, status);
        return OrderResponse.fromEntity(updatedOrder);
    }
}