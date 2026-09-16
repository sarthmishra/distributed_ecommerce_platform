package com.ecommerce.platform.event.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorConfig.class);

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate, MeterRegistry meterRegistry) {
        Counter retryCounter = Counter.builder("kafka.retry")
                .description("Total number of Kafka consumer retry attempts")
                .register(meterRegistry);

        Counter dltCounter = Counter.builder("kafka.dlt")
                .description("Total number of events recovered to Dead Letter Topic")
                .register(meterRegistry);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> {
                    log.error("Exhausted retries for record key [{}] on topic [{}]. Publishing to DLT [{}]",
                            record.key(), record.topic(), KafkaTopicConfig.TOPIC_ORDERS_DLT);
                    dltCounter.increment();
                    return new TopicPartition(KafkaTopicConfig.TOPIC_ORDERS_DLT, 0);
                });

        // 3 total attempts: 1 initial attempt + 2 retries with 1000ms delay
        FixedBackOff backOff = new FixedBackOff(1000L, 2L);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            retryCounter.increment();
            log.warn("Retry attempt {} for record key [{}] on topic [{}] due to error: {}",
                    deliveryAttempt, record.key(), record.topic(), ex.getMessage());
        });

        return errorHandler;
    }
}
