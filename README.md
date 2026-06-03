# Order Processing System

An event-driven order processing system built with Java 21, Spring Boot, Apache Kafka, and MongoDB. Demonstrates service-to-service communication over Kafka topics, at-least-once delivery with manual offset commits, dead letter queue handling, and deliberate use of Java 21 sealed interfaces and records for a compile-time-safe event hierarchy.

---

## Architecture

`POST /orders` hits order-service, which persists the order to MongoDB and publishes an `OrderPlaced` event to Kafka. payment-service consumes that event, simulates payment processing, and publishes either a `PaymentProcessed` event (on success) or routes to the dead letter queue (on failure). order-service consumes `PaymentProcessed` and updates the order status. Services communicate exclusively via Kafka topics — no direct HTTP calls between them.

```
  ┌──────────────────────┐
  │     order-service    │  POST /orders
  │     (REST API :8080) │◄────── HTTP client
  └──────────┬───────────┘
             │ orders.placed
             ▼
  ┌──────────────────────┐
  │   payment-service    │
  │   (Kafka consumer)   │
  └──────┬───────┬───────┘
         │       │ (failure)
         │       ▼
         │   orders.dlq (Kafka)
         │   dead_letter (MongoDB)
         │
         │ payments.processed
         ▼
  ┌──────────────────────┐
  │     order-service    │  updates order status
  │  (Kafka consumer)    │──────────────────────► MongoDB (orders)
  └──────────────────────┘
```

> fulfillment-service and notification-service coming in Stage 2.

---

## Tech Stack

| Layer            | Technology                          |
|------------------|-------------------------------------|
| Language         | Java 21                             |
| Framework        | Spring Boot 3                       |
| Messaging        | Apache Kafka (Confluent CP 7.6.0)   |
| Database         | MongoDB 7.0                         |
| Build            | Maven 3.9+                          |
| Containerisation | Docker Compose                      |
| Testing          | JUnit 5, Mockito                    |

---

## Java 21 Features

**Sealed interface for the event hierarchy (`OrderEvent`)**
`OrderEvent` is a sealed interface permitting exactly `OrderPlaced`, `PaymentProcessed`, and `OrderFulfilled`. The compiler enforces exhaustiveness — if a new event type is added to the hierarchy, every switch expression that dispatches on `OrderEvent` becomes a compile error until the new case is handled. This makes the codebase structurally sound: you cannot forget to handle a new event type.

**Pattern matching switch for consumer dispatch**
Consumer logic uses `switch (event) { case OrderPlaced op -> ... }` instead of a chain of `instanceof` checks followed by explicit casts. The pattern variable is scoped to its branch and the cast is implied, removing a class of bugs where a cast and the preceding `instanceof` check get out of sync during refactoring. Pattern matching switch on sealed interfaces is stable from Java 21 (JEP 441) — this is why the project targets Java 21 rather than Java 17, where it was only a preview feature.

**Records for all event POJOs**
`OrderPlaced`, `PaymentProcessed`, and `OrderFulfilled` are records. They are immutable by construction, implement `equals`/`hashCode`/`toString` correctly, and require no boilerplate. A Kafka event that mutates after it is published is a bug waiting to happen — records make that class of bug impossible.

---

## Key Engineering Decisions

- **Kafka over RabbitMQ.** Kafka's event log retention model maps naturally to the event sourcing pattern used here — every event is also written to the `event_log` collection in MongoDB. Kafka is also the dominant choice in Java backend roles, making it the more relevant thing to demonstrate.

- **Manual offset commit.** `enable.auto.commit=false` means offsets are committed only after the MongoDB write succeeds. Auto-commit can acknowledge a message before it has been processed; if the service crashes in that window, the event is silently lost. Manual commit ensures at-least-once delivery — the worst case is a redelivery, not a drop.

- **Idempotency check in payment-service.** Before processing any `OrderPlaced` event, payment-service checks the `event_log` collection for the event ID. If it already exists, the event is a redelivery and is skipped. This makes at-least-once delivery safe in practice: the first delivery succeeds, subsequent ones are no-ops.

- **`common` module as the single source of truth for event types.** Duplicating `OrderPlaced` across services means the two copies can diverge — different field names, different types — causing silent deserialization failures. One shared module, one definition.

- **MongoDB for order documents.** An order has nested line items, a shipping address, and a status field. In a SQL schema that is at least three tables and joins on every read. In MongoDB it is one document. The schema also stays flexible — adding a payment reference or tracking number does not require a migration.

---

## Running Locally

### Prerequisites

- Java 21
- Maven 3.9+
- Docker Desktop

### Start the system

```bash
git clone <repo>
cd order-processing
docker compose up --build
```

The first build takes 3–5 minutes while Maven downloads dependencies. Kafka takes around 30 seconds to become healthy after containers start — the services wait for it automatically via `depends_on` healthchecks.

### Place a test order

```bash
curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-001",
    "lineItems": [
      { "productId": "prod-abc", "quantity": 2, "unitPrice": 29.99 }
    ],
    "shippingAddress": {
      "street": "123 Main St",
      "city": "Toronto",
      "postalCode": "M1A1A1",
      "country": "Canada"
    }
  }'
```

Returns `201 Created` with the new order ID.

### Check order status

```bash
curl http://localhost:8080/orders/{id}/status
```

Within a few seconds the status transitions from `PLACED` to `PAYMENT_PROCESSED` (or `FAILED` — payment-service simulates a 20% failure rate by default).

### Stop

```bash
docker compose down
```

---

## Project Structure

```
order-processing/
├── common/                  # Sealed OrderEvent interface and permitted records
├── order-service/           # REST API (POST /orders, GET /orders/{id}), Kafka producer + consumer
├── payment-service/         # Kafka consumer, payment simulation, DLQ publisher
├── fulfillment-service/     # (Stage 2 — coming soon)
├── notification-service/    # (Stage 2 — coming soon)
├── docker-compose.yml
└── CLAUDE.md
```

Each service is an independent Maven module. `common` is a shared library declared as a dependency in every service's `pom.xml`.

---

## Running the Tests

```bash
# All modules
mvn test

# order-service only
mvn test -pl order-service -am

# payment-service only
mvn test -pl payment-service -am
```

13 unit tests across two services (JUnit 5 + Mockito). Integration tests with Testcontainers for Kafka and MongoDB are planned for Stage 2.
