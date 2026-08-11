package com.ecommerce.platform.payment;

import com.ecommerce.platform.payment.model.Account;
import com.ecommerce.platform.payment.model.AccountType;
import com.ecommerce.platform.payment.model.JournalEntry;
import com.ecommerce.platform.payment.service.LedgerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Transactional
public class LedgerServiceTest {

    @Autowired
    private LedgerService ledgerService;

    private Account customerAccount;
    private Account merchantAccount;
    private Account systemSourceAccount;

    @BeforeEach
    void setUp() {
        systemSourceAccount = ledgerService.createAccount("system@bank.com", AccountType.SYSTEM_SETTLEMENT);
        customerAccount = ledgerService.createAccount("user@example.com", AccountType.CUSTOMER_WALLET);
        merchantAccount = ledgerService.createAccount("merchant@store.com", AccountType.MERCHANT_REVENUE);

        // Initial deposit: System -> Customer Wallet ₹5000.00
        ledgerService.recordTransfer(
                "INIT-DEPOSIT-100",
                systemSourceAccount.getAccountNumber(),
                customerAccount.getAccountNumber(),
                new BigDecimal("5000.00"),
                "Initial wallet top-up"
        );
    }

    @Test
    @DisplayName("Should verify initial deposit balance for customer wallet")
    void testInitialBalance() {
        BigDecimal balance = ledgerService.calculateAccountBalance(customerAccount);
        assertEquals(new BigDecimal("5000.00"), balance);
    }

    @Test
    @DisplayName("Should execute double-entry transfer between customer and merchant")
    void testDoubleEntryTransfer() {
        BigDecimal transferAmount = new BigDecimal("1200.00");
        
        JournalEntry journal = ledgerService.recordTransfer(
                "TXN-ORDER-999",
                customerAccount.getAccountNumber(),
                merchantAccount.getAccountNumber(),
                transferAmount,
                "Payment for order #999"
        );

        assertNotNull(journal);
        assertEquals(2, journal.getEntries().size());

        // Check updated balances
        BigDecimal customerBalance = ledgerService.calculateAccountBalance(customerAccount);
        BigDecimal merchantBalance = ledgerService.calculateAccountBalance(merchantAccount);

        assertEquals(new BigDecimal("3800.00"), customerBalance);
        assertEquals(new BigDecimal("1200.00"), merchantBalance);
    }

    @Test
    @DisplayName("Should throw IllegalStateException when customer has insufficient wallet balance")
    void testInsufficientWalletBalance() {
        BigDecimal excessiveAmount = new BigDecimal("10000.00");

        assertThrows(IllegalStateException.class, () -> {
            ledgerService.recordTransfer(
                    "TXN-ORDER-FAIL",
                    customerAccount.getAccountNumber(),
                    merchantAccount.getAccountNumber(),
                    excessiveAmount,
                    "Payment attempt exceeding balance"
            );
        });
    }
}
