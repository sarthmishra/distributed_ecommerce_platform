package com.ecommerce.platform.outbox.repository;

import com.ecommerce.platform.outbox.model.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {
    List<OutboxEvent> findTop100ByProcessedFalseOrderByCreatedAtAsc();
}
