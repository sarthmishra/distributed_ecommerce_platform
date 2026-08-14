# Distributed E-Commerce Platform

A Spring Boot backend demonstrating **event-driven architecture, the Transactional Outbox Pattern, Saga orchestration, Kafka-based asynchronous communication, concurrency-safe inventory management, and a double-entry financial ledger**.

The project is currently implemented as a **modular monolith** that uses distributed-system patterns internally. It is designed to model how a real e-commerce backend can coordinate orders, payments, inventory, and asynchronous events reliably.

---

## Architecture

```text
                         REST Client
                             |
                             v
                     +---------------+
                     | OrderController|
                     +-------+-------+
                             |
                             v
                     +---------------+
                     |  OrderService |
                     +-------+-------+
                             |
              +--------------+--------------+
              |                             |
              v                             v
        +-----------+                +-------------+
        | Orders DB |                | Outbox Table|
        +-----------+                +------+------+
                                            |
                                            v
                                  +--------------------+
                                  | Outbox Publisher   |
                                  | @Scheduled Poller  |
                                  +---------+----------+
                                            |
                                            v
                                      +-----------+
                                      |   Kafka   |
                                      +-----+-----+
                                            |
                                            v
                                  +-------------------+
                                  | Saga Orchestrator |
                                  +---------+---------+
                                            |
                         +------------------+------------------+
                         |                                     |
                         v                                     v
                  +-------------+                       +-------------+
                  |   Payment   |                       |  Inventory  |
                  |   / Ledger  |                       |   Service   |
                  +------+------+                       +------+------+
                         |                                     |
                         v                                     v
                  Payment Event                         Inventory Event
                         |                                     |
                         +------------------+------------------+
                                            |
                                            v
                                      Order Status
                                       COMPLETED
```

---

## Main Technologies

* **Java**
* **Spring Boot**
* **Spring Data JPA**
* **Hibernate**
* **Apache Kafka**
* **H2 Database**
* **Maven**
* **JUnit 5**
* **Spring Kafka Test**
* **Awaitility**
* **Docker Compose**

---

## Core Concepts Demonstrated

### 1. Transactional Outbox Pattern

Creating an order and publishing an event to Kafka are separate operations.

Publishing directly to Kafka after saving the order could create a dual-write problem.

Instead:

```text
Order creation
     |
     +----> Orders table
     |
     +----> Outbox table
```

Both are written inside the same database transaction.

The outbox event is later published asynchronously to Kafka.

```text
OrderService
     |
     v
Database Transaction
     |
     +--> Order
     |
     +--> OutboxEvent
              |
              v
     OutboxPublisherService
              |
              v
            Kafka
```

This means that if Kafka is temporarily unavailable, the event remains stored in the database and can be retried later.

---

## 2. Kafka Event-Driven Communication

The application uses Kafka topics to communicate between stages of the order workflow.

Current topics include:

```text
orders.created
payments.completed
payments.failed
inventory.reserved
inventory.failed
orders.dlt
```

The main event flow is:

```text
OrderCreatedEvent
        |
        v
orders.created
        |
        v
Saga Step 1
        |
        v
Payment
        |
        v
PaymentCompletedEvent
        |
        v
payments.completed
        |
        v
Saga Step 2
        |
        v
Inventory Reservation
```

---

## 3. Saga Orchestration

The application uses a **Saga Orchestrator** rather than attempting to make the entire workflow one database transaction.

### Successful flow

```text
PENDING
   |
   v
Payment
   |
   v
PAYMENT_COMPLETED
   |
   v
Inventory Reservation
   |
   v
COMPLETED
```

### Payment failure

```text
PENDING
   |
   v
Payment Failure
   |
   v
PAYMENT_FAILED
```

### Inventory failure

If payment succeeds but inventory reservation fails:

```text
PENDING
   |
   v
PAYMENT_COMPLETED
   |
   v
Inventory Failure
   |
   v
Compensating Refund
   |
   v
CANCELLED
```

The refund is a **compensating transaction**, because a Saga does not provide a global ACID rollback across separate operations.

---

## 4. Idempotent Saga Processing

Kafka messages can potentially be delivered more than once.

The Saga therefore uses order state as an idempotency guard.

For example, an `OrderCreatedEvent` should only process an order that is currently:

```text
PENDING
```

If the order has already moved to another state, duplicate processing is skipped.

Similarly:

```text
PaymentCompletedEvent
```

only proceeds when the order is:

```text
PAYMENT_COMPLETED
```

This prevents duplicate payment processing, inventory reservation, or refunds.

---

## 5. Double-Entry Financial Ledger

The payment subsystem models financial transfers using ledger entries.

A transfer follows the basic double-entry principle:

```text
Customer Wallet       -₹4000
Merchant Revenue     +₹4000
--------------------------------
Net                    ₹0
```

Every financial movement therefore has corresponding debit and credit entries.

The system also uses idempotency records to prevent the same transfer request from being processed multiple times.

---

## 6. Concurrency-Safe Inventory

Inventory reservation uses database locking to prevent concurrent requests from overselling the same product.

The important operation uses a pessimistic write lock:

```text
SELECT product FOR UPDATE
```

Conceptually:

```text
Transaction A
    |
    v
Lock Product
    |
    v
Check Stock
    |
    v
Reduce Stock
    |
    v
Commit
```

Another transaction attempting to modify the same product must wait for the lock.

---

# End-to-End Order Flow

A normal order follows this complete path:

```text
1. Client
      |
      | POST /api/v1/orders
      v
2. OrderController
      |
      v
3. OrderService
      |
      +----------------------+
      |                      |
      v                      v
4. Orders Table        5. Outbox Table
                             |
                             v
6. OutboxPublisherService
                             |
                             v
7. Kafka: orders.created
                             |
                             v
8. SagaOrchestrator
                             |
                             v
9. Payment / Ledger
                             |
                             v
10. Order = PAYMENT_COMPLETED
                             |
                             v
11. Kafka: payments.completed
                             |
                             v
12. SagaOrchestrator
                             |
                             v
13. Inventory Reservation
                             |
                             v
14. Order = COMPLETED
```

---

# Example

The E2E test creates:

```text
Product:
SKU      = LAPTOP-E2E-01
Price    = ₹2000
Stock    = 5

Customer:
priya@example.com

Wallet:
₹10000
```

Then creates an order for:

```text
Quantity = 2
```

Order value:

```text
2 × ₹2000 = ₹4000
```

Expected result:

```text
Customer balance:
₹10000 - ₹4000 = ₹6000

Inventory:
5 - 2 = 3

Order:
COMPLETED
```

---

# Project Structure

```text
src/
├── main/
│   ├── java/
│   │   └── com/ecommerce/platform/
│   │
│   │       ├── common/
│   │       │   ├── dto/
│   │       │   └── exception/
│   │       │
│   │       ├── event/
│   │       │   ├── config/
│   │       │   ├── consumer/
│   │       │   ├── dto/
│   │       │   └── publisher/
│   │       │
│   │       ├── inventory/
│   │       │   ├── controller/
│   │       │   ├── dto/
│   │       │   ├── model/
│   │       │   ├── repository/
│   │       │   └── service/
│   │       │
│   │       ├── order/
│   │       │   ├── controller/
│   │       │   ├── dto/
│   │       │   ├── model/
│   │       │   ├── repository/
│   │       │   └── service/
│   │       │
│   │       ├── outbox/
│   │       │   ├── model/
│   │       │   ├── repository/
│   │       │   └── service/
│   │       │
│   │       ├── payment/
│   │       │   ├── controller/
│   │       │   ├── dto/
│   │       │   ├── model/
│   │       │   ├── repository/
│   │       │   └── service/
│   │       │
│   │       └── saga/
│   │           └── service/
│   │
│   └── resources/
│       └── application.yml
│
└── test/
    └── java/
        └── com/ecommerce/platform/
            ├── e2e/
            ├── event/
            ├── inventory/
            ├── order/
            ├── payment/
            └── saga/
```

---

# Important Components

### OrderService

Responsible for:

* Creating orders
* Calculating order totals
* Persisting orders
* Creating `OrderCreatedEvent`
* Persisting the event into the outbox

The order and outbox event are created in the same transaction.

---

### OutboxPublisherService

Responsible for:

* Finding unprocessed outbox events
* Publishing them to Kafka
* Marking successfully published events as processed

The publisher runs periodically using Spring scheduling.

---

### EventPublisher

Provides the Kafka publishing abstraction used by the application.

It publishes events to the appropriate Kafka topic.

---

### SagaOrchestrator

Coordinates the asynchronous order workflow.

Responsibilities include:

* Processing `OrderCreatedEvent`
* Charging the customer
* Publishing `PaymentCompletedEvent`
* Handling payment failures
* Reserving inventory
* Handling inventory failures
* Executing compensating refunds
* Protecting against duplicate event processing

---

### InventoryService

Responsible for:

* Product creation
* Product lookup
* Stock reservation
* Concurrency-safe inventory updates

Inventory reservation uses pessimistic database locking.

---

### LedgerService

Responsible for:

* Account creation
* Transfers
* Ledger entries
* Balance calculation
* Financial transaction integrity

---

# Testing

The project contains multiple levels of testing.

## Unit Tests

Examples:

```text
OrderServiceTest
InventoryServiceTest
LedgerServiceTest
```

These test individual business components.

## Integration Tests

Examples:

```text
SagaOrchestrationIntegrationTest
KafkaEventDrivenIntegrationTest
```

These verify larger pieces of the event-driven architecture.

## End-to-End Test

```text
OrderCreationEndToEndTest
```

This is the most important wiring test.

It sends a real HTTP request:

```text
POST /api/v1/orders
```

and verifies the complete:

```text
REST
→ OrderService
→ Outbox
→ Kafka
→ Saga
→ Payment
→ Kafka
→ Inventory
→ COMPLETED
```

flow.

It does not directly invoke the Saga orchestrator.

---

# Running the Project

## Start dependencies

The project includes Docker Compose configuration.

```bash
docker compose up -d
```

## Run the application

```bash
mvn spring-boot:run
```

The application runs on:

```text
http://localhost:8080
```

## Run tests

```bash
mvn test
```

Run the end-to-end test specifically:

```bash
mvn test -Dtest=OrderCreationEndToEndTest
```

---

# Current Development Database

The current configuration uses an in-memory H2 database:

```text
jdbc:h2:mem:ecommercedb
```

The H2 console is enabled for development/testing.

The project can later be migrated to PostgreSQL for a more production-like setup.

---

# Current Limitations

This project currently demonstrates distributed-system patterns inside a modular monolith.

It is **not yet a collection of independently deployed microservices**.

Potential future improvements include:

* PostgreSQL
* Redis
* API Gateway
* JWT authentication
* Resilience4j circuit breakers
* Dead Letter Topic processing
* More robust outbox concurrency handling
* Prometheus metrics
* Grafana dashboards
* Distributed tracing
* Dockerized application deployment
* Independent microservice deployment
* Stronger production-grade idempotency
* Retry and backoff policies

These features should only be documented as implemented after they actually exist in the codebase.

---

# Bug Fixes / Reliability Improvements

Important reliability bugs that have already been addressed include:

### Outbox Bug

Orders were previously able to exist without a corresponding `ORDER_CREATED` outbox event.

**Fixed by:** writing the order and outbox event inside the same transaction.

### Account Lookup Bug

The Saga could look up financial accounts using the wrong identity field.

**Fixed by:** adding lookup by:

```text
userEmail + accountType
```

### Duplicate Event Processing

Kafka redelivery could cause repeated Saga operations.

**Fixed by:** state-based idempotency guards.

### Inventory Race Condition

Concurrent stock reservations could potentially oversell inventory.

**Fixed by:** pessimistic database locking.

### Missing End-to-End Verification

Older tests could call Saga methods directly and therefore miss wiring problems.

**Fixed by:** adding a real HTTP-based E2E test covering the complete event-driven workflow.

---

# Design Principles

The project intentionally demonstrates the following backend engineering principles:

* Separation of responsibilities
* Transactional consistency
* Event-driven communication
* Eventual consistency
* Idempotent message processing
* Compensating transactions
* Database concurrency control
* Double-entry accounting
* Asynchronous processing
* Integration testing
* End-to-end testing

---

# Author

**Sarth Mishra**

B.Tech Computer Science & Engineering

This project is being developed as a backend/system-design focused portfolio project demonstrating practical Spring Boot and distributed-systems concepts.
