package com.ecommerce.platform.event.consumer;

import com.ecommerce.platform.event.config.KafkaTopicConfig;
import com.ecommerce.platform.event.dto.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    private final Counter dltCounter;

    public EventConsumer(MeterRegistry meterRegistry) {
        this.dltCounter = Counter.builder("orders.dlt")
                .description("Total number of events landed in Dead Letter Topic")
                .register(meterRegistry);
    }

    public void consumeOrderCreated(OrderCreatedEvent event) {
        consumeOrderCreated(event, null);
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_ORDERS_CREATED, groupId = "ecommerce-group")
    public void consumeOrderCreated(OrderCreatedEvent event,
                                    @Header(name = "X-Correlation-ID", required = false) String correlationId) {
        setupMdc(correlationId, event.orderNumber());
        try {
            log.info("Received OrderCreatedEvent: Order {}", event.orderNumber());
        } finally {
            clearMdc();
        }
    }

    public void consumePaymentCompleted(PaymentCompletedEvent event) {
        consumePaymentCompleted(event, null);
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_PAYMENTS_COMPLETED, groupId = "ecommerce-group")
    public void consumePaymentCompleted(PaymentCompletedEvent event,
                                        @Header(name = "X-Correlation-ID", required = false) String correlationId) {
        setupMdc(correlationId, event.orderNumber());
        try {
            log.info("Received PaymentCompletedEvent: Transaction {} for Order {}", event.transactionId(), event.orderNumber());
        } finally {
            clearMdc();
        }
    }

    public void consumePaymentFailed(PaymentFailedEvent event) {
        consumePaymentFailed(event, null);
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_PAYMENTS_FAILED, groupId = "ecommerce-group")
    public void consumePaymentFailed(PaymentFailedEvent event,
                                     @Header(name = "X-Correlation-ID", required = false) String correlationId) {
        setupMdc(correlationId, event.orderNumber());
        try {
            log.warn("Received PaymentFailedEvent: Order {} failed due to {}", event.orderNumber(), event.reason());
        } finally {
            clearMdc();
        }
    }

    public void consumeInventoryReserved(InventoryReservedEvent event) {
        consumeInventoryReserved(event, null);
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_INVENTORY_RESERVED, groupId = "ecommerce-group")
    public void consumeInventoryReserved(InventoryReservedEvent event,
                                         @Header(name = "X-Correlation-ID", required = false) String correlationId) {
        setupMdc(correlationId, event.orderNumber());
        try {
            log.info("Received InventoryReservedEvent: Reserved stock for Order {}", event.orderNumber());
        } finally {
            clearMdc();
        }
    }

    public void consumeInventoryFailed(InventoryFailedEvent event) {
        consumeInventoryFailed(event, null);
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_INVENTORY_FAILED, groupId = "ecommerce-group")
    public void consumeInventoryFailed(InventoryFailedEvent event,
                                      @Header(name = "X-Correlation-ID", required = false) String correlationId) {
        setupMdc(correlationId, event.orderNumber());
        try {
            log.error("Received InventoryFailedEvent: Stock reservation failed for Order {} - {}", event.orderNumber(), event.reason());
        } finally {
            clearMdc();
        }
    }

    public void consumeDeadLetterTopic(ConsumerRecord<String, Object> record,
                                       String originalTopic,
                                       String exceptionMessage) {
        consumeDeadLetterTopic(record, originalTopic, exceptionMessage, null);
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_ORDERS_DLT, groupId = "ecommerce-dlt-group")
    public void consumeDeadLetterTopic(ConsumerRecord<String, Object> record,
                                       @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) String originalTopic,
                                       @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage,
                                       @Header(name = "X-Correlation-ID", required = false) String correlationId) {
        setupMdc(correlationId, record.key());
        try {
            dltCounter.increment();
            log.error("Received Dead Letter Event on topic [{}] from original topic [{}] with key [{}]. Reason: {}",
                    record.topic(), originalTopic, record.key(), exceptionMessage);
        } finally {
            clearMdc();
        }
    }

    private void setupMdc(String correlationId, String orderNumber) {
        if (StringUtils.hasText(correlationId)) {
            MDC.put("correlationId", correlationId);
        }
        if (StringUtils.hasText(orderNumber)) {
            MDC.put("orderNumber", orderNumber);
        }
    }

    private void clearMdc() {
        MDC.remove("correlationId");
        MDC.remove("orderNumber");
    }
}
