package com.ecommerce.platform.payment.service;

import com.ecommerce.platform.payment.model.IdempotentRecord;
import com.ecommerce.platform.payment.repository.IdempotencyRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class IdempotencyService {

    private final IdempotencyRepository idempotencyRepository;

    public IdempotencyService(IdempotencyRepository idempotencyRepository) {
        this.idempotencyRepository = idempotencyRepository;
    }

    @Transactional(readOnly = true)
    public Optional<IdempotentRecord> getRecord(String idempotencyKey) {
        return idempotencyRepository.findByIdempotencyKey(idempotencyKey);
    }

    @Transactional
    public IdempotentRecord saveRecord(String idempotencyKey, String requestHash, int responseCode, String responseBody) {
        IdempotentRecord record = new IdempotentRecord(idempotencyKey, requestHash, responseCode, responseBody);
        return idempotencyRepository.save(record);
    }
}
