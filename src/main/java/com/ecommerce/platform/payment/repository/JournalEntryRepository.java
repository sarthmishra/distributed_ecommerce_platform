package com.ecommerce.platform.payment.repository;

import com.ecommerce.platform.payment.model.JournalEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface JournalEntryRepository extends JpaRepository<JournalEntry, Long> {
    Optional<JournalEntry> findByTransactionId(String transactionId);
    boolean existsByTransactionId(String transactionId);
}
