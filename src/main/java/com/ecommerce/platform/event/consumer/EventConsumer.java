package com.ecommerce.platform.event.consumer;

import com.ecommerce.platform.event.config.KafkaTopicConfig;
import com.ecommerce.platform.event.dto.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_ORDERS_CREATED, groupId = "ecommerce-group")
    public void consumeOrderCreated(OrderCreatedEvent event) {
        log.info("Received OrderCreatedEvent: Order {}", event.orderNumber());
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_PAYMENTS_COMPLETED, groupId = "ecommerce-group")
    public void consumePaymentCompleted(PaymentCompletedEvent event) {
        log.info("Received PaymentCompletedEvent: Transaction {} for Order {}", event.transactionId(), event.orderNumber());
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_PAYMENTS_FAILED, groupId = "ecommerce-group")
    public void consumePaymentFailed(PaymentFailedEvent event) {
        log.warn("Received PaymentFailedEvent: Order {} failed due to {}", event.orderNumber(), event.reason());
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_INVENTORY_RESERVED, groupId = "ecommerce-group")
    public void consumeInventoryReserved(InventoryReservedEvent event) {
        log.info("Received InventoryReservedEvent: Reserved stock for Order {}", event.orderNumber());
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_INVENTORY_FAILED, groupId = "ecommerce-group")
    public void consumeInventoryFailed(InventoryFailedEvent event) {
        log.error("Received InventoryFailedEvent: Stock reservation failed for Order {} - {}", event.orderNumber(), event.reason());
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_ORDERS_DLT, groupId = "ecommerce-dlt-group")
    public void consumeDeadLetterTopic(ConsumerRecord<String, Object> record,
                                       @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) String originalTopic,
                                       @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage) {
        log.error("Received Dead Letter Event on topic [{}] from original topic [{}] with key [{}]. Reason: {}",
                record.topic(), originalTopic, record.key(), exceptionMessage);
    }
}
