package com.ecommerce.platform.event.publisher;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Service
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);
    public static final String CORRELATION_ID_HEADER = "X-Correlation-ID";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public EventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishEvent(String topic, String key, Object eventPayload) {
        String correlationId = MDC.get("correlationId");
        publishEvent(topic, key, eventPayload, correlationId);
    }

    public void publishEvent(String topic, String key, Object eventPayload, String correlationId) {
        if (!StringUtils.hasText(correlationId)) {
            correlationId = MDC.get("correlationId");
        }

        log.info(
                "Publishing event to topic [{}] with key [{}] and correlation ID [{}]: {}",
                topic,
                key,
                correlationId,
                eventPayload
        );

        try {
            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, eventPayload);
            if (StringUtils.hasText(correlationId)) {
                record.headers().add(CORRELATION_ID_HEADER, correlationId.getBytes(StandardCharsets.UTF_8));
            }

            SendResult<String, Object> result =
                    kafkaTemplate.send(record)
                            .get(10, TimeUnit.SECONDS);

            log.info(
                    "Successfully published event to topic [{}] partition [{}] offset [{}]",
                    topic,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset()
            );

        } catch (Exception ex) {
            log.error(
                    "Failed to publish event to topic [{}] with key [{}]: {}",
                    topic,
                    key,
                    ex.getMessage(),
                    ex
            );

            throw new IllegalStateException(
                    "Kafka publish failed for topic " + topic,
                    ex
            );
        }
    }
}