package com.ecommerce.platform.event;

import com.ecommerce.platform.event.config.KafkaTopicConfig;
import com.ecommerce.platform.event.dto.OrderCreatedEvent;
import com.ecommerce.platform.event.publisher.EventPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9096", "port=9096" })
public class KafkaRetryAndDltIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaRetryAndDltIntegrationTest.class);

    @Autowired
    private EventPublisher eventPublisher;

    @Autowired
    private TestFlakyConsumer testFlakyConsumer;

    @Autowired
    private TestDltConsumer testDltConsumer;

    @Test
    @DisplayName("Transient listener failure should retry and eventually succeed")
    void testTransientFailure_RetriesAndSucceeds() {
        String orderNumber = "ORD-RETRY-TRANSIENT-001";
        OrderCreatedEvent event = new OrderCreatedEvent(
                orderNumber, "test@example.com", "SKU-RETRY-1", 1, new BigDecimal("100.00")
        );

        eventPublisher.publishEvent(KafkaTopicConfig.TOPIC_ORDERS_CREATED, orderNumber, event);

        // Await until consumer receives initial attempt AND retry attempt
        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    assertTrue(testFlakyConsumer.getAttemptCount(orderNumber) >= 2, "Expected at least 2 attempts (initial + retry)");
                    assertTrue(testFlakyConsumer.isCompleted(orderNumber), "Expected consumer to eventually succeed after retry");
                });
    }

    @Test
    @DisplayName("Permanent listener failure should exhaust retries and publish to DLT with preserved headers")
    void testPermanentFailure_ExhaustsRetriesAndLandsInDlt() {
        String orderNumber = "ORD-RETRY-PERMANENT-999";
        OrderCreatedEvent event = new OrderCreatedEvent(
                orderNumber, "fail@example.com", "SKU-FAIL", 1, new BigDecimal("500.00")
        );

        eventPublisher.publishEvent(KafkaTopicConfig.TOPIC_ORDERS_CREATED, orderNumber, event);

        // Await until DLT consumer receives the exhausted event on orders.dlt
        await().atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {
                    assertTrue(testDltConsumer.getDltReceivedKeys().containsKey(orderNumber), "Expected event key in DLT received map");
                    assertEquals(KafkaTopicConfig.TOPIC_ORDERS_CREATED, testDltConsumer.getOriginalTopic(orderNumber));
                    assertNotNull(testDltConsumer.getExceptionMessage(orderNumber));
                });
    }

    @TestConfiguration
    static class TestConsumerConfig {

        @Bean
        public TestFlakyConsumer testFlakyConsumer() {
            return new TestFlakyConsumer();
        }

        @Bean
        public TestDltConsumer testDltConsumer() {
            return new TestDltConsumer();
        }
    }

    public static class TestFlakyConsumer {
        private final ConcurrentHashMap<String, Integer> attempts = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Boolean> completed = new ConcurrentHashMap<>();

        @KafkaListener(topics = KafkaTopicConfig.TOPIC_ORDERS_CREATED, groupId = "retry-test-group")
        public void consume(ConsumerRecord<String, Object> record) {
            String key = record.key();
            if (key == null) return;

            int attempt = attempts.merge(key, 1, Integer::sum);
            log.info("TestFlakyConsumer processing key [{}] attempt {}", key, attempt);

            if (key.startsWith("ORD-RETRY-TRANSIENT")) {
                if (attempt == 1) {
                    throw new RuntimeException("Simulated transient failure on attempt 1");
                }
                completed.put(key, true);
                log.info("TestFlakyConsumer succeeded on attempt {} for key [{}]", attempt, key);
            } else if (key.startsWith("ORD-RETRY-PERMANENT")) {
                throw new RuntimeException("Simulated permanent failure for DLT test");
            }
        }

        public int getAttemptCount(String key) {
            return attempts.getOrDefault(key, 0);
        }

        public boolean isCompleted(String key) {
            return completed.getOrDefault(key, false);
        }
    }

    public static class TestDltConsumer {
        private final ConcurrentHashMap<String, String> originalTopics = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, String> exceptionMessages = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Boolean> dltReceivedKeys = new ConcurrentHashMap<>();

        @KafkaListener(topics = KafkaTopicConfig.TOPIC_ORDERS_DLT, groupId = "dlt-test-group")
        public void consumeDlt(ConsumerRecord<String, Object> record,
                               @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) String originalTopic,
                               @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage) {
            log.info("TestDltConsumer received DLT record key [{}] from topic [{}] with error: {}",
                    record.key(), originalTopic, exceptionMessage);
            if (record.key() != null) {
                dltReceivedKeys.put(record.key(), true);
                if (originalTopic != null) originalTopics.put(record.key(), originalTopic);
                if (exceptionMessage != null) exceptionMessages.put(record.key(), exceptionMessage);
            }
        }

        public ConcurrentHashMap<String, Boolean> getDltReceivedKeys() { return dltReceivedKeys; }
        public String getOriginalTopic(String key) { return originalTopics.get(key); }
        public String getExceptionMessage(String key) { return exceptionMessages.get(key); }
    }
}
