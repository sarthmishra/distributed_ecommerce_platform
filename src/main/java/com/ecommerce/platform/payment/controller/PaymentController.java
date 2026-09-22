package com.ecommerce.platform.payment.controller;

import com.ecommerce.platform.common.dto.ApiResponse;
import com.ecommerce.platform.common.exception.ResourceNotFoundException;
import com.ecommerce.platform.payment.dto.TransferRequest;
import com.ecommerce.platform.payment.model.Account;
import com.ecommerce.platform.payment.model.AccountType;
import com.ecommerce.platform.payment.model.IdempotentRecord;
import com.ecommerce.platform.payment.model.JournalEntry;
import com.ecommerce.platform.payment.repository.AccountRepository;
import com.ecommerce.platform.payment.service.IdempotencyService;
import com.ecommerce.platform.payment.service.LedgerService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/payments")
public class PaymentController {

    private final LedgerService ledgerService;
    private final AccountRepository accountRepository;
    private final IdempotencyService idempotencyService;

    public PaymentController(LedgerService ledgerService,
                             AccountRepository accountRepository,
                             IdempotencyService idempotencyService) {
        this.ledgerService = ledgerService;
        this.accountRepository = accountRepository;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping("/accounts")
    public ResponseEntity<ApiResponse<Account>> createAccount(@RequestParam String userEmail, @RequestParam AccountType accountType) {
        Account account = ledgerService.createAccount(userEmail, accountType);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(ApiResponse.success("Account created successfully", account));
    }

    @GetMapping("/accounts/{accountNumber}/balance")
    public ResponseEntity<ApiResponse<BigDecimal>> getBalance(@PathVariable String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new ResourceNotFoundException("Account not found: " + accountNumber));

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.isAuthenticated() && !"anonymousUser".equals(auth.getPrincipal())) {
            boolean isAdmin = auth.getAuthorities().stream().anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN"));
            if (!isAdmin && !account.getUserEmail().equalsIgnoreCase(auth.getName())) {
                throw new AccessDeniedException("Access denied: You do not own this account");
            }
        }

        BigDecimal balance = ledgerService.calculateAccountBalance(account);
        return ResponseEntity.ok(ApiResponse.success("Balance retrieved successfully", balance));
    }

    @PostMapping("/transfer")
    public ResponseEntity<ApiResponse<String>> transfer(
            @RequestHeader(value = "X-Idempotency-Key", required = false) String idempotencyKey,
            @Valid @RequestBody TransferRequest request) {

        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            Optional<IdempotentRecord> existingRecord = idempotencyService.getRecord(idempotencyKey);
            if (existingRecord.isPresent()) {
                return ResponseEntity.status(existingRecord.get().getResponseCode())
                        .body(ApiResponse.success("Idempotent response (cached)", existingRecord.get().getResponseBody()));
            }
        }

        String transactionId = "TXN-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        JournalEntry journalEntry = ledgerService.recordTransfer(
                transactionId,
                request.fromAccountNumber(),
                request.toAccountNumber(),
                request.amount(),
                request.description()
        );

        String successMessage = "Transfer successful with transaction ID: " + journalEntry.getTransactionId();

        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            idempotencyService.saveRecord(idempotencyKey, request.toString(), 200, journalEntry.getTransactionId());
        }

        return ResponseEntity.ok(ApiResponse.success(successMessage, journalEntry.getTransactionId()));
    }
}
