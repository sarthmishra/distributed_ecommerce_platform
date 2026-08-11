package com.ecommerce.platform.event;

import com.ecommerce.platform.event.config.KafkaTopicConfig;
import com.ecommerce.platform.event.dto.OrderCreatedEvent;
import com.ecommerce.platform.event.publisher.EventPublisher;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9093", "port=9093" })
public class KafkaEventDrivenIntegrationTest {

    @Autowired
    private EventPublisher eventPublisher;

    @Test
    @DisplayName("Should publish OrderCreatedEvent to Embedded Kafka broker cleanly")
    void testPublishOrderCreatedEvent() {
        OrderCreatedEvent event = new OrderCreatedEvent(
                "ORD-KAFKA-001",
                "kafka-user@example.com",
                "PROD-SKU-99",
                2,
                new BigDecimal("299.99")
        );

        assertDoesNotThrow(() -> {
            eventPublisher.publishEvent(KafkaTopicConfig.TOPIC_ORDERS_CREATED, event.orderNumber(), event);
        });
    }
}
