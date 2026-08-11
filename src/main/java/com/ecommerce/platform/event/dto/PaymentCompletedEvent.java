package com.ecommerce.platform.event.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record PaymentCompletedEvent(
        String transactionId,
        String orderNumber,
        String customerEmail,
        BigDecimal amount,
        LocalDateTime timestamp
) {
    public PaymentCompletedEvent(String transactionId, String orderNumber, String customerEmail, BigDecimal amount) {
        this(transactionId, orderNumber, customerEmail, amount, LocalDateTime.now());
    }
}
