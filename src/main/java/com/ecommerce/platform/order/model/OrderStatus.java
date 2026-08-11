package com.ecommerce.platform.order.model;

public enum OrderStatus {
    PENDING,
    PAYMENT_PROCESSING,
    PAYMENT_COMPLETED,
    PAYMENT_FAILED,
    INVENTORY_RESERVED,
    INVENTORY_FAILED,
    COMPLETED,
    CANCELLED
}
