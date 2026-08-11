package com.ecommerce.platform.event.dto;

import java.time.LocalDateTime;

public record InventoryFailedEvent(
        String orderNumber,
        String productSku,
        String reason,
        LocalDateTime timestamp
) {
    public InventoryFailedEvent(String orderNumber, String productSku, String reason) {
        this(orderNumber, productSku, reason, LocalDateTime.now());
    }
}
