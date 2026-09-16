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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final InventoryService inventoryService;
    private final OutboxPublisherService outboxPublisherService;
    private final ObjectMapper objectMapper;
    private final Counter ordersCreatedCounter;

    public OrderService(OrderRepository orderRepository,
                        InventoryService inventoryService,
                        OutboxPublisherService outboxPublisherService,
                        ObjectMapper objectMapper,
                        MeterRegistry meterRegistry) {
        this.orderRepository = orderRepository;
        this.inventoryService = inventoryService;
        this.outboxPublisherService = outboxPublisherService;
        this.objectMapper = objectMapper;
        this.ordersCreatedCounter = Counter.builder("orders.created")
                .description("Total number of orders created")
                .register(meterRegistry);
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
        ordersCreatedCounter.increment();
        log.info("Order created successfully: {} for customer: {}", savedOrder.getOrderNumber(), savedOrder.getCustomerEmail());

        OrderCreatedEvent event = new OrderCreatedEvent(
                savedOrder.getOrderNumber(),
                savedOrder.getCustomerEmail(),
                savedOrder.getProductSku(),
                savedOrder.getQuantity(),
                savedOrder.getTotalAmount()
        );

        String correlationId = MDC.get("correlationId");

        try {
            String payload = objectMapper.writeValueAsString(event);
            outboxPublisherService.saveToOutbox("ORDER", savedOrder.getOrderNumber(), "ORDER_CREATED", payload, correlationId);
        } catch (JsonProcessingException ex) {
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
        log.info("Updated status for order {} to {}", orderNumber, status);
        return OrderResponse.fromEntity(updatedOrder);
    }
}