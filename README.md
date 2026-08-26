# Distributed E-Commerce Platform

A Spring Boot backend demonstrating **event-driven architecture, the Transactional Outbox Pattern, Saga orchestration, Kafka-based asynchronous communication, Dead Letter Topic (DLT) retry handling, concurrency-safe inventory management, double-entry ledger accounting, and PostgreSQL persistence**.

The project is implemented as a **modular monolith** that uses distributed-system patterns internally. It is designed to model how an e-commerce backend coordinates orders, payments, inventory, and asynchronous messaging reliably.

---

## 1. Project Overview

In distributed architectures, managing multi-step business transactions across distinct domain boundaries presents reliability challenges: dual-write inconsistencies, duplicate message deliveries, race conditions during inventory allocation, and lack of distributed ACID transactions.

This project implements a backend solution within a modular monolith structure using proven distributed-system patterns:
- **Transactional Outbox** to eliminate dual-write hazards between database state and message broker events.
- **Orchestrated Saga Pattern** to coordinate multi-step transactions across Orders, Payments, and Inventory with compensating refund capabilities.
- **Pessimistic Locking** on database rows to prevent inventory overselling under high concurrency.
- **Double-Entry Financial Ledger** ensuring balanced debit/credit accounting for all customer and merchant wallet movements.
- **Idempotency Guards** at both message-handling and database levels to prevent double-charging or duplicate side-effects from Kafka redeliveries.
- **Consumer Retry & Dead Letter Topic (DLT)** for resilient failure handling and unprocessable message routing.

---

## 2. Key Features

- **Spring Boot 3 Backend**: Built on Java 21 and Spring Boot 3.4.2.
- **RESTful Endpoints**: Clean API contracts for product creation, order placement, and ledger account management.
- **Spring Data JPA & Hibernate**: Entity lifecycle management and customized dialect handling.
- **Dual Database Strategy**:
  - **PostgreSQL**: Production-grade relational database for persistent development and integration verification.
  - **H2 (In-Memory)**: Fast execution environment for unit and component-level testing.
- **Transactional Outbox Pattern**: Atomic persistence of domain state (`Order`) and events (`OutboxEvent`) in a single database transaction, published to Kafka via a scheduled poller (`OutboxPublisherService`).
- **Apache Kafka Messaging**: Asynchronous event-driven communication decoupling the order creation, payment, and inventory domains.
- **Saga Orchestrator**: State-machine orchestration managing the order lifecycle across `PENDING`, `PAYMENT_COMPLETED`, `PAYMENT_FAILED`, `COMPLETED`, and `CANCELLED`.
- **Compensating Transactions**: Automatic refund execution via ledger reversal when downstream inventory reservation fails.
- **Pessimistic Concurrency Control**: `@Lock(LockModeType.PESSIMISTIC_WRITE)` to enforce row-level locking on inventory (`SELECT ... FOR UPDATE`).
- **Double-Entry Financial Ledger**: Immutable journal and ledger entries enforcing zero-sum financial integrity (`Debits == Credits`).
- **State-Based & Key-Based Idempotency**: Order status checks prevent redundant Saga transitions, while `IdempotencyService` caches API response payloads against idempotency keys.
- **Kafka Consumer Error Handling**: Spring Kafka `DefaultErrorHandler` configured with fixed backoff retries (3 total attempts) and `DeadLetterPublishingRecoverer` routing failed messages to `orders.dlt` with error headers.
- **Comprehensive Test Suite**: 18 automated tests spanning unit tests, Spring context integration tests, embedded Kafka message flow tests, retry/DLT tests, real REST end-to-end flow, and live PostgreSQL persistence tests.

---

## 3. Architecture

```mermaid
flowchart TD
    subgraph ClientLayer [Client]
        Client[HTTP Client / Frontend]
    end

    subgraph OrderDomain [Order Service]
        Controller[OrderController<br/>POST /api/v1/orders]
        OrderSvc[OrderService]
    end

    subgraph DatabaseLayer [Relational Storage - PostgreSQL / H2]
        OrderTable[(orders)]
        OutboxTable[(outbox_events)]
        InventoryTable[(products)]
        LedgerTable[(accounts / journal_entries / ledger_entries)]
        IdempotencyTable[(idempotent_records)]
    end

    subgraph OutboxWorker [Outbox Relaying]
        OutboxPublisher[OutboxPublisherService<br/>@Scheduled Poller]
    end

    subgraph MessagingLayer [Apache Kafka Broker]
        TopicCreated[orders.created]
        TopicPaid[payments.completed]
        TopicPayFailed[payments.failed]
        TopicReserved[inventory.reserved]
        TopicInvFailed[inventory.failed]
        TopicDLT[orders.dlt]
    end

    subgraph SagaLayer [Saga Orchestration]
        Saga[SagaOrchestrator]
        ErrorHandler[DefaultErrorHandler<br/>FixedBackOff: 2 Retries]
        DLTRecoverer[DeadLetterPublishingRecoverer]
    end

    subgraph PaymentDomain [Payment & Ledger Subsystem]
        LedgerSvc[LedgerService]
    end

    subgraph InventoryDomain [Inventory Subsystem]
        InventorySvc[InventoryService]
    end

    %% Client flow
    Client -->|1. Create Order Request| Controller
    Controller --> OrderSvc

    %% Atomic local transaction
    OrderSvc -->|2. Atomic DB Transaction| OrderTable
    OrderSvc -->|2. Atomic DB Transaction| OutboxTable

    %% Outbox Poller
    OutboxTable -.->|3. Poll Unprocessed| OutboxPublisher
    OutboxPublisher -->|4. Publish Event| TopicCreated

    %% Saga Flow Step 1: Payment
    TopicCreated -->|5. Consume| Saga
    Saga -->|6. Debit Customer / Credit Merchant| LedgerSvc
    LedgerSvc --> LedgerTable
    Saga -->|7a. Payment Success| TopicPaid
    Saga -->|7b. Payment Failed| TopicPayFailed

    %% Saga Flow Step 2: Inventory
    TopicPaid -->|8. Consume| Saga
    Saga -->|9. Reserve Stock with Pessimistic Lock| InventorySvc
    InventorySvc --> InventoryTable
    Saga -->|10a. Stock Reserved| TopicReserved
    Saga -->|10b. Stock Insufficient| TopicInvFailed

    %% Compensation
    TopicInvFailed -->|11. Trigger Compensation| Saga
    Saga -->|12. Execute Refund Transfer| LedgerSvc

    %% Error Handling & DLT
    Saga -.->|Listener Exception| ErrorHandler
    ErrorHandler -->|Exhausted Retries| DLTRecoverer
    DLTRecoverer -->|Publish with Headers| TopicDLT
```

> **Note on Architecture**: The project is structured as a **modular monolith** with clear domain separation (`order`, `payment`, `inventory`, `outbox`, `event`, `saga`). It does not deploy domains as independent microservice artifacts; instead, it demonstrates distributed architectural patterns inside a unified Spring Boot application.

---

## 4. Distributed-System Patterns

### Transactional Outbox Pattern
Writing to a database and publishing an event to a message broker cannot be combined in a single ACID transaction without two-phase commit (2PC) protocols, which introduce performance and availability bottlenecks.
* **Mechanism**: When an order is placed, `OrderService` inserts the `Order` record and an `OutboxEvent` record into the database within the same `@Transactional` boundary.
* **Guarantee**: If the transaction succeeds, the event is guaranteed to be in the database. `OutboxPublisherService` periodically queries unprocessed events (`processed = false`), publishes them to Kafka, and marks them processed upon broker acknowledgment.

### Saga Orchestration Pattern
Long-running business workflows spanning multiple domains avoid distributed database locks by using an orchestrator that coordinates a series of local transactions:
1. **Step 1 (Payment)**: On `OrderCreatedEvent`, the orchestrator charges the customer's wallet and updates the order to `PAYMENT_COMPLETED`.
2. **Step 2 (Inventory Allocation)**: On `PaymentCompletedEvent`, the orchestrator reserves stock for the requested SKU and marks the order `COMPLETED`.
3. **Compensating Action (Refund)**: If stock reservation fails, an `InventoryFailedEvent` is published. The orchestrator intercepts this event, issues a reverse transfer from the merchant back to the customer, and transitions the order to `CANCELLED`.

### Idempotency
Message brokers provide at-least-once delivery guarantees, meaning duplicate events can occur during network blips or consumer rebalances:
* **Saga State Guards**: `SagaOrchestrator` checks the current order status prior to processing. `handleOrderCreated` only processes orders in `PENDING` status; `handlePaymentCompleted` only processes orders in `PAYMENT_COMPLETED` status; compensating refunds skip orders that are already `CANCELLED`.
* **Idempotent API Cache**: `IdempotencyService` records incoming request hashes against a unique `idempotencyKey` in the `idempotent_records` table, returning cached responses for identical duplicate requests.

### Pessimistic Concurrency Control
To prevent overselling when multiple customers concurrently purchase the same item:
* `ProductRepository.findBySkuWithLock()` executes `@Lock(LockModeType.PESSIMISTIC_WRITE)`.
* In PostgreSQL, this translates to `SELECT ... FOR UPDATE` (or `FOR NO KEY UPDATE`), preventing race conditions by serializing concurrent updates at the database row level.

### Double-Entry Financial Ledger
Financial movements are tracked using immutable accounting records:
* Every financial operation creates a `JournalEntry` containing at least two balanced `LedgerEntry` lines: one debit and one credit.
* Account balances are derived mathematically from ledger entries (`Credits - Debits` for liabilities/wallets, `Debits - Credits` for assets/settlement).

### Kafka Retry & Dead Letter Topic (DLT)
To handle transient and permanent consumer errors:
* `KafkaErrorConfig` configures a Spring Kafka `DefaultErrorHandler` with `FixedBackOff(1000L, 2L)` (1 initial attempt + 2 retries, 1-second delay).
* If all retries fail, `DeadLetterPublishingRecoverer` forwards the unprocessable message to `orders.dlt`.
* Original metadata is preserved in Kafka headers: `kafka_dlt-original-topic`, `kafka_dlt-exception-message`, and `kafka_dlt-exception-stacktrace`.

---

## 5. Technology Stack

| Category | Technology | Version / Specification |
| :--- | :--- | :--- |
| **Language** | Java | OpenJDK 21 |
| **Framework** | Spring Boot | 3.4.2 |
| **Data Access** | Spring Data JPA / Hibernate | Hibernate 6 (Spring Boot Starter JPA) |
| **Message Broker** | Apache Kafka | Spring Kafka 3.3.2 / Confluent Platform 7.5.0 |
| **Relational Database** | PostgreSQL | PostgreSQL 16 (Alpine Container) |
| **In-Memory Database** | H2 Database | 2.3.232 (Test scope / default profile) |
| **Testing** | JUnit 5, Spring Boot Test | JUnit Jupiter 5.11.4 |
| **Embedded Testing** | Spring Kafka Test, Awaitility | EmbeddedKafka Broker, Awaitility 4.2.2 |
| **Containerization** | Docker, Docker Compose | Compose file version 3.8 |
| **Build Tool** | Apache Maven | 3.9+ |

---

## 6. Database Architecture

### Entities

| Entity | Table Name | Purpose | Key Attributes |
| :--- | :--- | :--- | :--- |
| **Order** | `orders` | Order state and customer purchase details | `orderNumber`, `customerEmail`, `productSku`, `quantity`, `totalAmount`, `status`, `version` |
| **Product** | `products` | Product catalog and inventory stock tracking | `sku`, `name`, `price`, `stockQuantity` |
| **Account** | `accounts` | Double-entry financial account definition | `accountNumber`, `userEmail`, `accountType` (`CUSTOMER_WALLET`, `MERCHANT_REVENUE`, `SYSTEM_SETTLEMENT`) |
| **JournalEntry** | `journal_entries` | Transaction grouping container | `transactionId`, `description`, `createdAt` |
| **LedgerEntry** | `ledger_entries` | Individual debit/credit balance line | `entryType` (`DEBIT`, `CREDIT`), `amount`, `account_id`, `journal_entry_id` |
| **OutboxEvent** | `outbox_events` | Transactional outbox event records | `aggregateType`, `aggregateId`, `eventType`, `payload` (TEXT), `processed` |
| **IdempotentRecord** | `idempotent_records` | Request/response cache for idempotent operations | `idempotencyKey`, `requestHash`, `responseCode`, `responseBody` (TEXT) |

### Environment Configuration

* **Default Profile (H2 In-Memory)**: Configured in `src/main/resources/application.yml`. Runs in-memory with `H2Dialect` for rapid development and isolated automated unit/integration test runs.
* **PostgreSQL Profile (`postgres`)**: Configured in `src/main/resources/application-postgres.yml`. Connects to PostgreSQL using `PostgreSQLDialect` with `hibernate.ddl-auto: update`.

To activate the PostgreSQL profile:
```bash
mvn spring-boot:run -Dspring-boot.run.profiles=postgres
```
Or via environment variable:
```bash
export SPRING_PROFILES_ACTIVE=postgres
```

---

## 7. Kafka & Event Flow

### Topics

| Topic Name | Producer | Consumer | Payload |
| :--- | :--- | :--- | :--- |
| `orders.created` | `OutboxPublisherService` | `SagaOrchestrator`, `EventConsumer` | `OrderCreatedEvent` |
| `payments.completed` | `SagaOrchestrator` | `SagaOrchestrator`, `EventConsumer` | `PaymentCompletedEvent` |
| `payments.failed` | `SagaOrchestrator` | `EventConsumer` | `PaymentFailedEvent` |
| `inventory.reserved` | `SagaOrchestrator` | `EventConsumer` | `InventoryReservedEvent` |
| `inventory.failed` | `SagaOrchestrator` | `SagaOrchestrator`, `EventConsumer` | `InventoryFailedEvent` |
| `orders.dlt` | `DeadLetterPublishingRecoverer` | `EventConsumer` | Original event payload with failure headers |

---

## 8. Saga Lifecycle & State Transitions

### Happy Path (Order Success)

```text
[POST /api/v1/orders]
       │
       ▼
   (PENDING) ──► OutboxEvent persisted atomically
       │
       ▼ [orders.created]
   Saga Step 1: Debit customer wallet, credit merchant
       │
       ▼
(PAYMENT_COMPLETED)
       │
       ▼ [payments.completed]
   Saga Step 2: Pessimistic lock product SKU & decrement stock
       │
       ▼
  (COMPLETED) ──► [inventory.reserved]
```

### Compensating Failure Path (Insufficient Stock)

```text
[POST /api/v1/orders]
       │
       ▼
   (PENDING)
       │
       ▼ [orders.created]
   Saga Step 1: Payment completes
       │
       ▼
(PAYMENT_COMPLETED)
       │
       ▼ [payments.completed]
   Saga Step 2: Stock check fails (requested > stockQuantity)
       │
       ▼ [inventory.failed]
   Compensating Action: Reverse transfer (Debit merchant, credit customer)
       │
       ▼
  (CANCELLED) ──► Customer balance fully restored
```

---

## 9. Automated Testing Suite

The repository contains **18 automated tests** covering all layers of the architecture:

```text
-------------------------------------------------------
 T E S T S
-------------------------------------------------------
Running com.ecommerce.platform.inventory.InventoryServiceTest
Tests run: 4, Failures: 0, Errors: 0, Skipped: 0

Running com.ecommerce.platform.order.OrderServiceTest
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0

Running com.ecommerce.platform.payment.LedgerServiceTest
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0

Running com.ecommerce.platform.event.KafkaEventDrivenIntegrationTest
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0

Running com.ecommerce.platform.saga.SagaOrchestrationIntegrationTest
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0

Running com.ecommerce.platform.e2e.OrderCreationEndToEndTest
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0

Running com.ecommerce.platform.event.KafkaRetryAndDltIntegrationTest
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0

Running com.ecommerce.platform.postgres.PostgresIntegrationTest
Tests run: 4, Failures: 0, Errors: 0, Skipped: 0

Results:
Tests run: 18, Failures: 0, Errors: 0, Skipped: 0
```

### Test Categories

1. **Unit & Domain Tests**:
   - `InventoryServiceTest`: Product creation, duplicate SKU rejection, and stock reservation boundaries.
   - `OrderServiceTest`: Order entity creation, calculation, and outbox event staging.
   - `LedgerServiceTest`: Double-entry accounting integrity, debit/credit journal creation, and balance calculations.
2. **Kafka Messaging & Reliability Integration Tests**:
   - `KafkaEventDrivenIntegrationTest`: Verifies outbox event publishing and Kafka topic transmission.
   - `KafkaRetryAndDltIntegrationTest`: Verifies consumer retry backoff behavior and routing to `orders.dlt` on permanent errors.
3. **Saga Orchestration Integration Tests**:
   - `SagaOrchestrationIntegrationTest`: Exercises multi-step happy-path completion and compensating refund flows using an embedded Kafka broker.
4. **End-to-End REST Test**:
   - `OrderCreationEndToEndTest`: Issues a real HTTP POST request to `/api/v1/orders` and asserts that the full pipeline (REST $\to$ Order $\to$ Outbox $\to$ Kafka $\to$ Saga $\to$ Payment $\to$ Inventory) executes to `COMPLETED`.
5. **PostgreSQL Persistence Tests**:
   - `PostgresIntegrationTest`: Runs against the live PostgreSQL database to verify native table DDL, atomic outbox persistence, pessimistic row locking (`SELECT FOR UPDATE`), ledger transfers, and idempotency record storage.

---

## 10. Running the Project

### Prerequisites
- Java 21 JDK
- Maven 3.9+
- Docker & Docker Compose

### 1. Start Infrastructure Dependencies
Start PostgreSQL, Kafka, ZooKeeper, and Redis:
```bash
docker compose up -d
```

Verify the containers are healthy:
```bash
docker compose ps
```

### 2. Run the Test Suite
Run all automated unit and integration tests:
```bash
mvn test
```

Run a specific test class (e.g., PostgreSQL integration test or E2E test):
```bash
mvn test -Dtest=PostgresIntegrationTest
mvn test -Dtest=OrderCreationEndToEndTest
mvn test -Dtest=KafkaRetryAndDltIntegrationTest
```

### 3. Run the Application

**Using Default In-Memory H2 Database:**
```bash
mvn spring-boot:run
```

**Using PostgreSQL Persistence Profile:**
```bash
mvn spring-boot:run -Dspring-boot.run.profiles=postgres
```

The application starts on `http://localhost:8080`.

---

## 11. Example Scenarios

### Scenario A: Successful Order Placement

1. **Initial Setup**:
   - Product: `SKU = PHONE-15`, Stock = `10`, Price = `₹1,000.00`
   - Customer Wallet (`alice@example.com`): `₹5,000.00`
2. **Action**:
   - Client sends `POST /api/v1/orders` with `{ "customerEmail": "alice@example.com", "productSku": "PHONE-15", "quantity": 2 }`
3. **Result**:
   - Order created in `PENDING` status; total = `₹2,000.00`.
   - Outbox event published to `orders.created`.
   - Saga initiates payment: Alice debited `₹2,000.00` (balance $\to$ `₹3,000.00`), merchant credited `₹2,000.00`.
   - Order moves to `PAYMENT_COMPLETED`.
   - Saga reserves stock: Product stock decremented from `10` to `8`.
   - Order moves to `COMPLETED`.

### Scenario B: Insufficient Stock Compensating Refund

1. **Initial Setup**:
   - Product: `SKU = LAPTOP-PRO`, Stock = `1`, Price = `₹2,000.00`
   - Customer Wallet (`bob@example.com`): `₹10,000.00`
2. **Action**:
   - Client sends `POST /api/v1/orders` with `{ "customerEmail": "bob@example.com", "productSku": "LAPTOP-PRO", "quantity": 5 }`
3. **Result**:
   - Order created in `PENDING` status; total = `₹10,000.00`.
   - Saga Step 1 succeeds: Bob debited `₹10,000.00` (balance $\to$ `₹0.00`), Order moves to `PAYMENT_COMPLETED`.
   - Saga Step 2 fails: Stock check detects only `1` available (needs `5`). `InventoryFailedEvent` is emitted.
   - Compensating Refund triggered: Merchant debited `₹10,000.00`, Bob credited `₹10,000.00` (balance restored $\to$ `₹10,000.00`).
   - Order status updated to `CANCELLED`.

---

## 12. Project Structure

```text
src/
├── main/
│   ├── java/com/ecommerce/platform/
│   │   ├── DistributedEcommerceApplication.java
│   │   ├── common/
│   │   │   ├── dto/ApiResponse.java
│   │   │   └── exception/GlobalExceptionHandler.java
│   │   ├── event/
│   │   │   ├── config/KafkaTopicConfig.java
│   │   │   ├── config/KafkaErrorConfig.java
│   │   │   ├── consumer/EventConsumer.java
│   │   │   ├── dto/OrderCreatedEvent.java
│   │   │   ├── dto/PaymentCompletedEvent.java
│   │   │   ├── dto/PaymentFailedEvent.java
│   │   │   ├── dto/InventoryReservedEvent.java
│   │   │   ├── dto/InventoryFailedEvent.java
│   │   │   └── publisher/EventPublisher.java
│   │   ├── inventory/
│   │   │   ├── controller/InventoryController.java
│   │   │   ├── dto/CreateProductRequest.java
│   │   │   ├── dto/ProductResponse.java
│   │   │   ├── model/Product.java
│   │   │   ├── repository/ProductRepository.java
│   │   │   └── service/InventoryService.java
│   │   ├── order/
│   │   │   ├── controller/OrderController.java
│   │   │   ├── dto/CreateOrderRequest.java
│   │   │   ├── dto/OrderResponse.java
│   │   │   ├── model/Order.java
│   │   │   ├── model/OrderStatus.java
│   │   │   ├── repository/OrderRepository.java
│   │   │   └── service/OrderService.java
│   │   ├── outbox/
│   │   │   ├── model/OutboxEvent.java
│   │   │   ├── repository/OutboxEventRepository.java
│   │   │   └── service/OutboxPublisherService.java
│   │   ├── payment/
│   │   │   ├── controller/LedgerController.java
│   │   │   ├── dto/AccountResponse.java
│   │   │   ├── dto/TransferRequest.java
│   │   │   ├── model/Account.java
│   │   │   ├── model/AccountType.java
│   │   │   ├── model/EntryType.java
│   │   │   ├── model/IdempotentRecord.java
│   │   │   ├── model/JournalEntry.java
│   │   │   ├── model/LedgerEntry.java
│   │   │   ├── repository/AccountRepository.java
│   │   │   ├── repository/IdempotencyRepository.java
│   │   │   ├── repository/JournalEntryRepository.java
│   │   │   ├── repository/LedgerEntryRepository.java
│   │   │   ├── service/IdempotencyService.java
│   │   │   └── service/LedgerService.java
│   │   └── saga/
│   │       └── service/SagaOrchestrator.java
│   └── resources/
│       ├── application.yml
│       └── application-postgres.yml
└── test/
    └── java/com/ecommerce/platform/
        ├── e2e/OrderCreationEndToEndTest.java
        ├── event/KafkaEventDrivenIntegrationTest.java
        ├── event/KafkaRetryAndDltIntegrationTest.java
        ├── inventory/InventoryServiceTest.java
        ├── order/OrderServiceTest.java
        ├── payment/LedgerServiceTest.java
        ├── postgres/PostgresIntegrationTest.java
        └── saga/SagaOrchestrationIntegrationTest.java
```

---

## 13. Current Project Status

The project is currently a **functional demonstration of core distributed-system patterns** within a modular Spring Boot backend. It provides a reliable reference implementation for asynchronous transactions, dual-write prevention, concurrency-safe inventory manipulation, double-entry financial ledgering, consumer fault tolerance, and multi-database persistence.

It is not yet intended as an independently deployed, production-hardened microservice fleet.

---

## 14. Future Improvements

The following architectural enhancements are planned as future extensions:
- **Observability & Distributed Tracing**: OpenTelemetry instrumentation with Micrometer Tracing and Zipkin/Jaeger correlation IDs across Kafka headers.
- **Metrics & Dashboards**: Prometheus metrics export with Grafana dashboards for consumer lag, saga latency, and outbox throughput.
- **Authentication & Security**: Spring Security integration with JWT validation and role-based access control (RBAC).
- **API Documentation**: OpenAPI 3 / Swagger UI specification for all endpoints.
- **Centralized Outbox Debezium CDC**: Transitioning from scheduled polling to Change Data Capture (CDC) via Debezium and Kafka Connect.
- **CI/CD Pipelines**: Automated GitHub Actions workflow for build, unit test, and container image generation.

---

## 15. Development Checkpoints

1. `1a8d7ed` — *Fix saga payment and compensation flow*
2. `70676fc` — *Add Kafka retry and DLT handling*
3. `5904df6` — *Configure PostgreSQL persistence environment and integration tests*

---

## Author

**Sarth Mishra**
B.Tech Computer Science & Engineering
