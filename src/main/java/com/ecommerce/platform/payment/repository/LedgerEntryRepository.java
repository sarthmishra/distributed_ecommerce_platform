package com.ecommerce.platform.payment.repository;

import com.ecommerce.platform.payment.model.Account;
import com.ecommerce.platform.payment.model.EntryType;
import com.ecommerce.platform.payment.model.LedgerEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;

@Repository
public interface LedgerEntryRepository extends JpaRepository<LedgerEntry, Long> {

    @Query("SELECT COALESCE(SUM(l.amount), 0) FROM LedgerEntry l WHERE l.account = :account AND l.entryType = :entryType")
    BigDecimal sumAmountByAccountAndEntryType(Account account, EntryType entryType);
}
