package com.ecommerce.platform.security.service;

import com.ecommerce.platform.payment.model.AccountType;
import com.ecommerce.platform.payment.service.LedgerService;
import com.ecommerce.platform.security.dto.AuthResponse;
import com.ecommerce.platform.security.dto.LoginRequest;
import com.ecommerce.platform.security.dto.RegisterRequest;
import com.ecommerce.platform.security.model.Role;
import com.ecommerce.platform.security.model.User;
import com.ecommerce.platform.security.repository.UserRepository;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AuthService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtTokenProvider jwtTokenProvider;
    private final AuthenticationManager authenticationManager;
    private final LedgerService ledgerService;

    public AuthService(UserRepository userRepository,
                       PasswordEncoder passwordEncoder,
                       JwtTokenProvider jwtTokenProvider,
                       AuthenticationManager authenticationManager,
                       LedgerService ledgerService) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.jwtTokenProvider = jwtTokenProvider;
        this.authenticationManager = authenticationManager;
        this.ledgerService = ledgerService;
    }

    @Transactional
    public AuthResponse register(RegisterRequest request) {
        if (userRepository.existsByEmail(request.email())) {
            throw new IllegalArgumentException("Email is already registered: " + request.email());
        }

        String encodedPassword = passwordEncoder.encode(request.password());
        User user = new User(null, request.email(), encodedPassword, Role.ROLE_CUSTOMER);
        User savedUser = userRepository.save(user);

        // Auto-provision CUSTOMER_WALLET using existing ledger logic
        ledgerService.createAccount(savedUser.getEmail(), AccountType.CUSTOMER_WALLET);

        String token = jwtTokenProvider.generateToken(savedUser.getEmail(), savedUser.getRole().name());
        return new AuthResponse(token, savedUser.getEmail(), savedUser.getRole().name());
    }

    @Transactional(readOnly = true)
    public AuthResponse login(LoginRequest request) {
        authenticationManager.authenticate(
                new UsernamePasswordAuthenticationToken(request.email(), request.password())
        );

        User user = userRepository.findByEmail(request.email())
                .orElseThrow(() -> new IllegalArgumentException("User not found with email: " + request.email()));

        String token = jwtTokenProvider.generateToken(user.getEmail(), user.getRole().name());
        return new AuthResponse(token, user.getEmail(), user.getRole().name());
    }
}
