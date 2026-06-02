# Product Requirements Document
## Order Processing System

**Author:** Gurshaan Deol  
**Status:** Draft  
**Target:** Fall 2026 Internship Portfolio  
**Last Updated:** June 2026

---

## Overview

An event-driven order processing system built in Java 17 with Spring Boot and Apache Kafka. The system demonstrates production-grade patterns including sealed class event hierarchies, dead letter queues, and async inter-service communication via Kafka topics. MongoDB is used as the primary data store, taking advantage of its document model for nested order data.

This is a portfolio project. The goal is to demonstrate backend engineering depth — not to ship a product.

---

## Goals

- Demonstrate event-driven architecture with Kafka in a Java ecosystem
- Use Java 17 features (sealed classes, records, pattern matching) meaningfully — not cosmetically
- Show production awareness: dead letter queues, idempotency, error handling
- Produce at least one quotable metric (event throughput, latency) for the resume bullet
- Be completable in stages so it can go on the resume before it's fully built

---

## Non-Goals

- Frontend UI (no React, no dashboards — a status REST endpoint is enough)
- Authentication / JWT (already demonstrated in Workout Tracker)
- Payment gateway integration (mock payment logic only)
- Horizontal scaling or cloud deployment (Docker Compose local is sufficient)

---

## Services

### 1. Order Service
The entry point. Exposes a REST API to place and query orders. Publishes events to Kafka.

**Responsibilities:**
- Accept `POST /orders` with order payload (customer ID, line items, shipping address)
- Validate the request and persist the order to MongoDB with status `PLACED`
- Publish an `OrderPlaced` event to the `orders.placed` Kafka topic
- Expose `GET /orders/{id}` and `GET /orders/{id}/status` endpoints
- Expose `GET /metrics` — event count, avg processing time, DLQ count

**Java 17 usage:**
- `OrderEvent` as a sealed interface; `OrderPlaced`, `PaymentProcessed`, `OrderFulfilled` as permitted records
- Pattern matching with `switch` expressions when routing events

---

### 2. Payment Service
Consumes `orders.placed`. Simulates payment processing. Publishes result.

**Responsibilities:**
- Consume `OrderPlaced` events from `orders.placed`
- Simulate payment processing (random success/failure with configurable failure rate via env var)
- On success: update order status to `PAYMENT_PROCESSED`, publish `PaymentProcessed` to `payments.processed`
- On failure: publish to dead letter topic `orders.dlq` with error context
- Persist all payment events to MongoDB `event_log` collection

**Idempotency:** Check MongoDB for duplicate event IDs before processing — Kafka at-least-once delivery means the same event can arrive twice.

---

### 3. Fulfillment Service
Consumes `payments.processed`. Simulates order fulfillment.

**Responsibilities:**
- Consume `PaymentProcessed` events
- Simulate fulfillment (assign a mock tracking number)
- Update order status to `FULFILLED`
- Publish `OrderFulfilled` to `orders.fulfilled`

---

### 4. Notification Service
Consumes all three topics. Logs notifications (no actual email/SMS).

**Responsibilities:**
- Consume `orders.placed`, `payments.processed`, `orders.fulfilled`
- Log a structured notification message per event (simulates email/push)
- This service exists to demonstrate fan-out — multiple consumers on a single topic

---

## Kafka Topics

| Topic | Producer | Consumers | Retention |
|---|---|---|---|
| `orders.placed` | Order Service | Payment Service, Notification Service | 7 days |
| `payments.processed` | Payment Service | Fulfillment Service, Notification Service | 7 days |
| `orders.fulfilled` | Fulfillment Service | Notification Service | 7 days |
| `orders.dlq` | Payment Service | (manual inspection) | 30 days |

---

## Data Model

### MongoDB Collections

**`orders`**
```json
{
  "_id": "uuid",
  "customerId": "string",
  "status": "PLACED | PAYMENT_PROCESSED | FULFILLED | FAILED",
  "lineItems": [
    { "productId": "string", "quantity": 2, "unitPrice": 29.99 }
  ],
  "shippingAddress": { "street": "...", "city": "...", "postalCode": "..." },
  "trackingNumber": "string | null",
  "createdAt": "timestamp",
  "updatedAt": "timestamp"
}
```

**`event_log`**
```json
{
  "_id": "uuid",
  "eventId": "string",
  "eventType": "OrderPlaced | PaymentProcessed | OrderFulfilled",
  "orderId": "string",
  "payload": { ... },
  "processedAt": "timestamp",
  "serviceId": "string"
}
```

**`dead_letter`**
```json
{
  "_id": "uuid",
  "originalEventId": "string",
  "eventType": "string",
  "payload": { ... },
  "errorMessage": "string",
  "failedAt": "timestamp",
  "retryCount": 0
}
```

---

## Java 17 Feature Usage

These must appear in the codebase meaningfully — not just as syntax sugar.

| Feature | Where | Why |
|---|---|---|
| Sealed interfaces | `OrderEvent` hierarchy | Enforces exhaustive pattern matching at compile time |
| Records | All event POJOs | Immutable value types, no boilerplate |
| Pattern matching switch | Event router / consumer dispatch | Replaces instanceof chains cleanly |
| Text blocks | Kafka config / test fixtures | Cleaner multi-line strings |

---

## Error Handling

- Kafka consumer failures go to `orders.dlq` with full error context
- DLQ entries are persisted in MongoDB `dead_letter` collection
- Services log structured JSON (use Logback with JSON encoder)
- Failed events are NOT retried automatically — this is intentional simplicity

---

## Metrics Endpoint

`GET /metrics` on Order Service returns:

```json
{
  "totalOrders": 142,
  "ordersPlaced": 142,
  "paymentsProcessed": 138,
  "ordersFulfilled": 135,
  "dlqCount": 4,
  "avgPaymentProcessingMs": 47,
  "uptime": "2h 14m"
}
```

This is what goes on the resume as a quotable number.

---

## Build Stages

### Stage 1 — Resume-ready (target: 1-2 weeks)
- Order Service with REST API
- Kafka producer publishing `OrderPlaced`
- Payment Service consuming and publishing `PaymentProcessed`
- MongoDB persistence
- Docker Compose with Kafka + Zookeeper + MongoDB

### Stage 2 — While applying
- Fulfillment Service
- Notification Service (fan-out demo)
- Dead letter queue
- `/metrics` endpoint

### Stage 3 — Polish
- Idempotency checks
- Structured JSON logging
- Architecture diagram in README
- Load test script with throughput numbers

---

## Resume Bullet Targets

Once Stage 2 is complete, the bullets should read something like:

- Built an **event-driven order processing system** using Apache Kafka and Spring Boot, with 4 decoupled microservices communicating via 3 Kafka topics
- Modelled the event hierarchy using **Java 17 sealed interfaces and records** with exhaustive pattern matching across consumer dispatch logic
- Implemented a **dead letter queue** with MongoDB persistence for failed payment events, and a metrics endpoint reporting sub-50ms average processing latency
- Containerised all services with **Docker Compose** for single-command local deployment

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 17 |
| Framework | Spring Boot 3.x |
| Messaging | Apache Kafka |
| Database | MongoDB |
| Build | Maven |
| Container | Docker Compose |
| Testing | JUnit 5, Mockito, Testcontainers |
