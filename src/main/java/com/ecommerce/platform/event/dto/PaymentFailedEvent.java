package com.ecommerce.platform.event.dto;

import java.time.LocalDateTime;

public record PaymentFailedEvent(
        String orderNumber,
        String customerEmail,
        String reason,
        LocalDateTime timestamp
) {
    public PaymentFailedEvent(String orderNumber, String customerEmail, String reason) {
        this(orderNumber, customerEmail, reason, LocalDateTime.now());
    }
}
