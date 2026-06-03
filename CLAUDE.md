# CLAUDE.md — Order Processing System

This file tells Claude Code how to work on this project. Read it before touching any file.

---

## What this project is

An event-driven order processing system in Java 21 + Spring Boot + Kafka + MongoDB.
It is a portfolio project for a CS internship application. Code quality and clarity matter more than feature completeness.

---

## Project structure

```
order-processing/
├── order-service/          # REST API, Kafka producer + multi-topic consumer, GET /metrics
├── payment-service/        # Kafka consumer, payment logic, DLQ routing
├── fulfillment-service/    # Kafka consumer, fulfillment logic, OrderFulfilled publisher
├── notification-service/   # Kafka fan-out consumer on all three event topics
├── common/                 # Shared event types (sealed interfaces, records)
├── docker-compose.yml      # Kafka + Zookeeper + MongoDB + all services
├── load-test.sh            # Places 50 orders and reports throughput numbers
└── CLAUDE.md
```

Each service is an independent Maven module. The `common` module is a shared library imported by all services.

---

## Non-negotiable rules

**Java 21 features must be used meaningfully:**

- `OrderEvent` MUST be a sealed interface in the `common` module
- All event POJOs (OrderPlaced, PaymentProcessed, OrderFulfilled) MUST be records
- Consumer dispatch MUST use pattern matching switch expressions — no instanceof chains
- Do not add Java 21 features as decoration. If it doesn't make the code cleaner or safer, don't use it.

**MongoDB, not SQL:**

- This project uses MongoDB exclusively. Do not suggest PostgreSQL, H2, or any SQL database.
- Use Spring Data MongoDB. Do not use raw MongoClient unless there is a specific reason.
- Orders are stored as documents with nested line items — do not flatten them into relational-style structures.

**No authentication:**

- Do not add Spring Security, JWT, or any auth layer. It is already on the resume from another project.
- All endpoints are unauthenticated for simplicity.

**No frontend:**

- No HTML, no Thymeleaf, no React. REST endpoints and JSON responses only.

**Dead letter queue is required:**

- Payment failures MUST go to the `orders.dlq` Kafka topic AND be persisted to the MongoDB `dead_letter` collection.
- Do not silently swallow errors or log-and-continue without DLQ routing.

---

## Code style

- Package names: `com.gurshaandeol.orderprocessing.<service>`
- Class names: standard Java PascalCase
- No Lombok. Write out constructors and accessors explicitly, or use records where appropriate.
- No commented-out code. Delete it.
- Every public method needs a one-line Javadoc if its purpose isn't obvious from its name.
- Prefer small, focused classes. A class that does two things should probably be two classes.

---

## Kafka conventions

- Topic names: `orders.placed`, `payments.processed`, `orders.fulfilled`, `orders.dlq`
- Consumer group IDs: `payment-service-group`, `fulfillment-service-group`, `notification-service-group`
- All Kafka messages are JSON serialized. Use Jackson.
- Every consumer must handle deserialization errors gracefully — bad messages go to DLQ, not crashes.
- Consumer `enable.auto.commit=false` — commit offsets manually after successful processing.

---

## MongoDB conventions

- Collections: `orders`, `event_log`, `dead_letter`
- All documents use UUID string `_id` fields — not MongoDB ObjectId.
- Every document must have `createdAt` and `updatedAt` timestamps (use `Instant`).
- Use Spring Data MongoDB `@Document` annotations. No raw BSON manipulation.

---

## Docker Compose

- All services, Kafka, Zookeeper, and MongoDB must run with `docker compose up`.
- Services must wait for Kafka and MongoDB to be healthy before starting. Use `depends_on` with `healthcheck`.
- Environment variables for configuration (ports, topic names, connection strings) — no hardcoded values.
- Each service exposes a different host port. Order Service is on 8080.

---

## Testing

- Unit tests with JUnit 5 and Mockito for service logic.
- Integration tests with Testcontainers for Kafka and MongoDB — do not mock infrastructure in integration tests.
- Test class naming: `OrderServiceTest` (unit), `OrderServiceIntegrationTest` (integration).
- Aim for 80%+ coverage on service layer classes. Don't chase coverage on DTOs or config classes.

---

## What is built (all stages complete)

**Stage 1**
1. `common` module with sealed `OrderEvent` interface and record implementations
2. `order-service` — `POST /orders`, `GET /orders/{id}`, `GET /orders/{id}/status`, Kafka producer
3. `payment-service` — consumes `orders.placed`, publishes `payments.processed`, DLQ on failure
4. MongoDB persistence in both services
5. `docker-compose.yml` with Kafka + Zookeeper + MongoDB + all services

**Stage 2**
6. `fulfillment-service` — consumes `payments.processed`, publishes `orders.fulfilled`
7. `notification-service` — fan-out consumer on all three event topics
8. DLQ routing — payment failures go to `orders.dlq` (Kafka) and `dead_letter` (MongoDB)
9. `GET /metrics` on order-service — totalOrders, paymentsProcessed, ordersFulfilled, dlqCount, avgPaymentProcessingMs, uptime
10. Idempotency checks in payment-service and order-service via `event_log` collection
11. Structured JSON logging via logstash-logback-encoder in all four services
12. `load-test.sh` — places 50 orders, reports throughput and final status breakdown

**What the running system looks like**
- `docker compose up --build` starts all 7 containers with no errors
- `POST /orders` returns 201 with an order ID
- Order status flows: `PLACED` → `PAYMENT_PROCESSED` → `FULFILLED` (or `FAILED` + DLQ)
- All logs emit structured JSON with `@timestamp`, `level`, `service`, `message` fields
- `./load-test.sh` produces: ~7 orders/sec throughput, ~30ms avg payment latency

---

## Common mistakes to avoid

- Do not use `spring.kafka.consumer.auto-offset-reset=earliest` in production config — use it only in test/dev profiles
- Do not catch generic `Exception` in Kafka listeners — catch specific exceptions and route to DLQ
- Do not use `@Transactional` expecting distributed transaction behaviour across Kafka and MongoDB — it doesn't work that way
- Do not serialize the full `OrderEvent` sealed interface — serialize the concrete record type and include a `type` discriminator field in the JSON
- Do not block the Kafka listener thread with long-running operations — offload to a separate executor if needed

---

## The /metrics endpoint

Lives on Order Service at `GET /metrics`. Returns JSON with:

- `totalOrders`, `paymentsProcessed`, `ordersFulfilled`, `dlqCount`
- `avgPaymentProcessingMs` — computed from `event_log` timestamps
- `uptime`

This endpoint exists to give a quotable number for the resume. Make sure it's accurate.

---

## Questions to ask before writing code

1. Does this belong in `common` or in the specific service?
2. Am I using Java 21 features because they improve the code, or just to tick a box?
3. What happens to this code path if Kafka is down when the service starts?
4. What happens if the same event is delivered twice?
5. Does this error need to go to the DLQ or is it truly unrecoverable?
