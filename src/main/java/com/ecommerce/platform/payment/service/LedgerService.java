package com.ecommerce.platform.payment.service;

import com.ecommerce.platform.common.exception.ResourceNotFoundException;
import com.ecommerce.platform.payment.model.*;
import com.ecommerce.platform.payment.repository.AccountRepository;
import com.ecommerce.platform.payment.repository.JournalEntryRepository;
import com.ecommerce.platform.payment.repository.LedgerEntryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class LedgerService {

    private static final Logger log = LoggerFactory.getLogger(LedgerService.class);

    private final AccountRepository accountRepository;
    private final JournalEntryRepository journalEntryRepository;
    private final LedgerEntryRepository ledgerEntryRepository;

    public LedgerService(AccountRepository accountRepository,
                         JournalEntryRepository journalEntryRepository,
                         LedgerEntryRepository ledgerEntryRepository) {
        this.accountRepository = accountRepository;
        this.journalEntryRepository = journalEntryRepository;
        this.ledgerEntryRepository = ledgerEntryRepository;
    }

    @Transactional
    public Account createAccount(String userEmail, AccountType accountType) {
        String accountNumber = "ACC-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        Account account = new Account(null, accountNumber, userEmail, accountType, "INR");
        Account savedAccount = accountRepository.save(account);
        log.info("Created ledger account {} ({}) for {}", savedAccount.getAccountNumber(), accountType, userEmail);
        return savedAccount;
    }

    @Transactional(readOnly = true)
    public BigDecimal calculateAccountBalance(Account account) {
        BigDecimal totalCredits = ledgerEntryRepository.sumAmountByAccountAndEntryType(account, EntryType.CREDIT);
        BigDecimal totalDebits = ledgerEntryRepository.sumAmountByAccountAndEntryType(account, EntryType.DEBIT);
        
        // For wallets/revenue: Balance = Credits - Debits
        return totalCredits.subtract(totalDebits);
    }

    @Transactional
    public JournalEntry recordTransfer(String transactionId, String fromAccountNumber, String toAccountNumber, BigDecimal amount, String description) {
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Transfer amount must be strictly positive");
        }

        Account fromAccount = accountRepository.findByAccountNumberWithLock(fromAccountNumber)
                .orElseThrow(() -> new ResourceNotFoundException("Sender account not found: " + fromAccountNumber));
        
        Account toAccount = accountRepository.findByAccountNumberWithLock(toAccountNumber)
                .orElseThrow(() -> new ResourceNotFoundException("Recipient account not found: " + toAccountNumber));

        // Check sender balance if it's a customer wallet
        if (fromAccount.getAccountType() == AccountType.CUSTOMER_WALLET) {
            BigDecimal currentBalance = calculateAccountBalance(fromAccount);
            if (currentBalance.compareTo(amount) < 0) {
                throw new IllegalStateException("Insufficient funds in wallet " + fromAccountNumber + ". Current: " + currentBalance + ", Requested: " + amount);
            }
        }

        JournalEntry journalEntry = new JournalEntry(null, transactionId, description);

        // Double-Entry lines: Debit sender, Credit recipient
        LedgerEntry debitSender = new LedgerEntry(fromAccount, EntryType.DEBIT, amount);
        LedgerEntry creditRecipient = new LedgerEntry(toAccount, EntryType.CREDIT, amount);

        journalEntry.addLedgerEntry(debitSender);
        journalEntry.addLedgerEntry(creditRecipient);

        JournalEntry savedJournal = journalEntryRepository.save(journalEntry);
        log.info("Recorded double-entry transaction {}: Transferred {} from {} to {}", transactionId, amount, fromAccountNumber, toAccountNumber);
        
        return savedJournal;
    }
}
