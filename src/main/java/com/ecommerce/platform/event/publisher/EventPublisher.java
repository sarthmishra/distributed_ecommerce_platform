package com.ecommerce.platform.event.publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public EventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishEvent(String topic, String key, Object eventPayload) {
        log.info(
                "Publishing event to topic [{}] with key [{}]: {}",
                topic,
                key,
                eventPayload
        );

        try {
            SendResult<String, Object> result =
                    kafkaTemplate.send(topic, key, eventPayload)
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