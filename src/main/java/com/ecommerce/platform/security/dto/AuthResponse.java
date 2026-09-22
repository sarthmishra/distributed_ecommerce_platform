package com.ecommerce.platform.security.dto;

public record AuthResponse(
        String token,
        String tokenType,
        String email,
        String role
) {
    public AuthResponse(String token, String email, String role) {
        this(token, "Bearer", email, role);
    }
}
