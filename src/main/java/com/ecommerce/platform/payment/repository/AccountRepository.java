package com.ecommerce.platform.payment.repository;

import com.ecommerce.platform.payment.model.Account;
import com.ecommerce.platform.payment.model.AccountType;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AccountRepository extends JpaRepository<Account, Long> {

    Optional<Account> findByAccountNumber(String accountNumber);

    // method: findByUserEmailAndAccountType
    // does: looks up an account by the actual owner (email) + type, instead of
    //       mistakenly matching against accountNumber (which is a random "ACC-xxxx" string)
    Optional<Account> findByUserEmailAndAccountType(String userEmail, AccountType accountType);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT a FROM Account a WHERE a.accountNumber = :accountNumber")
    Optional<Account> findByAccountNumberWithLock(String accountNumber);

    boolean existsByAccountNumber(String accountNumber);
}