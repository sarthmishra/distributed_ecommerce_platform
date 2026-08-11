package com.ecommerce.platform.event.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record OrderCreatedEvent(
        String orderNumber,
        String customerEmail,
        String productSku,
        Integer quantity,
        BigDecimal totalAmount,
        LocalDateTime timestamp
) {
    public OrderCreatedEvent(String orderNumber, String customerEmail, String productSku, Integer quantity, BigDecimal totalAmount) {
        this(orderNumber, customerEmail, productSku, quantity, totalAmount, LocalDateTime.now());
    }
}
