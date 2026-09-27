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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "Payments & Ledger", description = "Double-entry ledger accounting, account balance queries, and idempotent fund transfers")
@SecurityRequirement(name = "BearerAuth")
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
    @Operation(summary = "Create a ledger account (Admin)", description = "Creates a new double-entry ledger account (CUSTOMER_WALLET, MERCHANT_REVENUE, or SYSTEM_SETTLEMENT). Requires ROLE_ADMIN.")
    @ApiResponses(value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "201", description = "Account created successfully"),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "401", description = "Unauthorized - Missing or invalid JWT"),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "403", description = "Forbidden - Requires ROLE_ADMIN")
    })
    public ResponseEntity<ApiResponse<Account>> createAccount(
            @Parameter(description = "User email associated with the account", example = "customer@example.com") @RequestParam String userEmail,
            @Parameter(description = "Type of account (CUSTOMER_WALLET, MERCHANT_REVENUE, SYSTEM_SETTLEMENT)", example = "CUSTOMER_WALLET") @RequestParam AccountType accountType) {
        Account account = ledgerService.createAccount(userEmail, accountType);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(ApiResponse.success("Account created successfully", account));
    }

    @GetMapping("/accounts/{accountNumber}/balance")
    @Operation(summary = "Get account balance", description = "Calculates the real-time balance of a ledger account (Credits minus Debits). Access restricted to account owner or ROLE_ADMIN.")
    @ApiResponses(value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "Balance calculated and retrieved successfully"),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "401", description = "Unauthorized - Missing or invalid JWT"),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "403", description = "Forbidden - Access denied: You do not own this account"),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "404", description = "Account not found")
    })
    public ResponseEntity<ApiResponse<BigDecimal>> getBalance(
            @Parameter(description = "Account number (e.g. ACC-12345678)", example = "ACC-A1B2C3D4") @PathVariable String accountNumber) {
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
    @Operation(summary = "Execute double-entry transfer (Admin)", description = "Records a balanced double-entry transfer between two accounts with optional X-Idempotency-Key header protection. Requires ROLE_ADMIN.")
    @ApiResponses(value = {
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "200", description = "Transfer recorded successfully"),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "400", description = "Invalid request payload or negative transfer amount"),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "401", description = "Unauthorized - Missing or invalid JWT"),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "403", description = "Forbidden - Requires ROLE_ADMIN"),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "404", description = "Sender or recipient account not found"),
            @io.swagger.v3.oas.annotations.responses.ApiResponse(responseCode = "409", description = "Conflict - Insufficient funds in sender wallet")
    })
    public ResponseEntity<ApiResponse<String>> transfer(
            @Parameter(description = "Optional unique client-provided key for idempotency protection", example = "IDEM-KEY-9999") @RequestHeader(value = "X-Idempotency-Key", required = false) String idempotencyKey,
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
