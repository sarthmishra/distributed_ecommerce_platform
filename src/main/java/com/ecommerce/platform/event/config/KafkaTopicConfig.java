package com.ecommerce.platform.event.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("!test")
public class KafkaTopicConfig {

    public static final String TOPIC_ORDERS_CREATED = "orders.created";
    public static final String TOPIC_PAYMENTS_COMPLETED = "payments.completed";
    public static final String TOPIC_PAYMENTS_FAILED = "payments.failed";
    public static final String TOPIC_INVENTORY_RESERVED = "inventory.reserved";
    public static final String TOPIC_INVENTORY_FAILED = "inventory.failed";
    public static final String TOPIC_ORDERS_DLT = "orders.dlt";

    @Bean
    public NewTopic ordersCreatedTopic() {
        return TopicBuilder.name(TOPIC_ORDERS_CREATED).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic paymentsCompletedTopic() {
        return TopicBuilder.name(TOPIC_PAYMENTS_COMPLETED).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic paymentsFailedTopic() {
        return TopicBuilder.name(TOPIC_PAYMENTS_FAILED).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryReservedTopic() {
        return TopicBuilder.name(TOPIC_INVENTORY_RESERVED).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryFailedTopic() {
        return TopicBuilder.name(TOPIC_INVENTORY_FAILED).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic ordersDltTopic() {
        return TopicBuilder.name(TOPIC_ORDERS_DLT).partitions(1).replicas(1).build();
    }
}
