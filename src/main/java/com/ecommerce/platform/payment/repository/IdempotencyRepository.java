package com.ecommerce.platform.payment.repository;

import com.ecommerce.platform.payment.model.IdempotentRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface IdempotencyRepository extends JpaRepository<IdempotentRecord, Long> {
    Optional<IdempotentRecord> findByIdempotencyKey(String idempotencyKey);
}
