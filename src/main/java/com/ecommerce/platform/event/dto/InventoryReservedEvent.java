package com.ecommerce.platform.event.dto;

import java.time.LocalDateTime;

public record InventoryReservedEvent(
        String orderNumber,
        String productSku,
        Integer quantity,
        LocalDateTime timestamp
) {
    public InventoryReservedEvent(String orderNumber, String productSku, Integer quantity) {
        this(orderNumber, productSku, quantity, LocalDateTime.now());
    }
}
