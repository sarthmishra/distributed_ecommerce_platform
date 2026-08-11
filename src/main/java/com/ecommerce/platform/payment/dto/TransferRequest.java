package com.ecommerce.platform.payment.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record TransferRequest(
        @NotBlank(message = "Sender account number is required")
        String fromAccountNumber,

        @NotBlank(message = "Recipient account number is required")
        String toAccountNumber,

        @NotNull(message = "Amount is required")
        @Positive(message = "Transfer amount must be greater than zero")
        BigDecimal amount,

        String description
) {}
