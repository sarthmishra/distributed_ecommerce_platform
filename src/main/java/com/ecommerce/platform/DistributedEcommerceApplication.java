package com.ecommerce.platform;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

// annotation: @EnableScheduling - turns on Spring's scheduling infrastructure.
// Without this, @Scheduled(fixedDelay = 2000) on OutboxPublisherService.processOutboxEvents()
// is silently ignored - the method is never invoked, so outbox rows are written but never
// published to Kafka. This is exactly why the E2E test stalls at PENDING.
@SpringBootApplication
@EnableScheduling
public class DistributedEcommerceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DistributedEcommerceApplication.class, args);
    }
}