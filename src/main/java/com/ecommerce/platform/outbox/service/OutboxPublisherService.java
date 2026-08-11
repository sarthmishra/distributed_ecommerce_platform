package com.ecommerce.platform.outbox.service;

import com.ecommerce.platform.event.config.KafkaTopicConfig;
import com.ecommerce.platform.event.dto.InventoryFailedEvent;
import com.ecommerce.platform.event.dto.InventoryReservedEvent;
import com.ecommerce.platform.event.dto.OrderCreatedEvent;
import com.ecommerce.platform.event.dto.PaymentCompletedEvent;
import com.ecommerce.platform.event.dto.PaymentFailedEvent;
import com.ecommerce.platform.event.publisher.EventPublisher;
import com.ecommerce.platform.outbox.model.OutboxEvent;
import com.ecommerce.platform.outbox.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class OutboxPublisherService {

    private static final Logger log =
            LoggerFactory.getLogger(OutboxPublisherService.class);

    private final OutboxEventRepository outboxEventRepository;
    private final EventPublisher eventPublisher;
    private final ObjectMapper objectMapper;

    public OutboxPublisherService(
            OutboxEventRepository outboxEventRepository,
            EventPublisher eventPublisher,
            ObjectMapper objectMapper
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.eventPublisher = eventPublisher;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public OutboxEvent saveToOutbox(
            String aggregateType,
            String aggregateId,
            String eventType,
            String jsonPayload
    ) {
        OutboxEvent outboxEvent =
                new OutboxEvent(
                        aggregateType,
                        aggregateId,
                        eventType,
                        jsonPayload
                );

        OutboxEvent savedEvent =
                outboxEventRepository.save(outboxEvent);

        log.info(
                "Saved outbox event [{}] for aggregate [{}:{}]",
                savedEvent.getId(),
                aggregateType,
                aggregateId
        );

        return savedEvent;
    }

    @Scheduled(fixedDelay = 2000)
    @Transactional
    public void processOutboxEvents() {

        List<OutboxEvent> pendingEvents =
                outboxEventRepository
                        .findTop100ByProcessedFalseOrderByCreatedAtAsc();

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info(
                "Found {} unprocessed outbox events to publish to Kafka",
                pendingEvents.size()
        );

        for (OutboxEvent event : pendingEvents) {

            String topic =
                    resolveTopicForAggregate(event.getAggregateType());

            try {

                /*
                 * The outbox stores the event payload as JSON text.
                 *
                 * Before sending it to Kafka, convert that JSON back
                 * into the correct Java event DTO.
                 *
                 * This allows Kafka's JsonSerializer to send the
                 * actual event object instead of a raw String.
                 */
                Object eventPayload =
                        deserializeEvent(
                                event.getEventType(),
                                event.getPayload()
                        );

                /*
                 * EventPublisher waits for Kafka acknowledgement.
                 *
                 * Therefore, if this method returns successfully,
                 * Kafka has accepted the message.
                 */
                eventPublisher.publishEvent(
                        topic,
                        event.getAggregateId(),
                        eventPayload
                );

                /*
                 * Only mark the outbox event as processed AFTER
                 * successful Kafka publication.
                 */
                event.setProcessed(true);

                outboxEventRepository.save(event);

                log.info(
                        "Outbox event [{}] successfully marked as processed",
                        event.getId()
                );

            } catch (Exception ex) {

                /*
                 * Leave processed=false.
                 *
                 * The scheduler will retry this event during
                 * the next polling cycle.
                 */
                log.error(
                        "Failed to publish outbox event [{}]: {}",
                        event.getId(),
                        ex.getMessage(),
                        ex
                );
            }
        }
    }

    private Object deserializeEvent(
            String eventType,
            String payload
    ) {
        try {

            return switch (eventType) {

                case "ORDER_CREATED" ->
                        objectMapper.readValue(
                                payload,
                                OrderCreatedEvent.class
                        );

                case "PAYMENT_COMPLETED" ->
                        objectMapper.readValue(
                                payload,
                                PaymentCompletedEvent.class
                        );

                case "PAYMENT_FAILED" ->
                        objectMapper.readValue(
                                payload,
                                PaymentFailedEvent.class
                        );

                case "INVENTORY_RESERVED" ->
                        objectMapper.readValue(
                                payload,
                                InventoryReservedEvent.class
                        );

                case "INVENTORY_FAILED" ->
                        objectMapper.readValue(
                                payload,
                                InventoryFailedEvent.class
                        );

                default ->
                        throw new IllegalArgumentException(
                                "Unknown outbox event type: " + eventType
                        );
            };

        } catch (Exception ex) {

            throw new IllegalStateException(
                    "Failed to deserialize outbox event type: "
                            + eventType,
                    ex
            );
        }
    }

    private String resolveTopicForAggregate(
            String aggregateType
    ) {

        return switch (aggregateType.toUpperCase()) {

            case "ORDER" ->
                    KafkaTopicConfig.TOPIC_ORDERS_CREATED;

            case "PAYMENT" ->
                    KafkaTopicConfig.TOPIC_PAYMENTS_COMPLETED;

            case "INVENTORY" ->
                    KafkaTopicConfig.TOPIC_INVENTORY_RESERVED;

            default ->
                    KafkaTopicConfig.TOPIC_ORDERS_CREATED;
        };
    }
}