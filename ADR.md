# Architecture Decision Records
## Order Processing System

ADRs document key decisions and the reasoning behind them. When Claude Code or a future reviewer asks "why did we do it this way", the answer is here.

---

## ADR-001: MongoDB over PostgreSQL

**Status:** Accepted

**Context:**  
The project already has a PostgreSQL + Spring Boot project on the resume (Workout Tracker). Adding another SQL project provides no differentiation. A NoSQL alternative would demonstrate breadth.

**Decision:**  
Use MongoDB as the primary data store.

**Reasoning:**  
Orders are a natural document use case. An order has nested line items, a shipping address, and status history — in SQL this requires at least 3 tables and joins. In MongoDB it's one document. The schema flexibility also means adding fields (e.g. tracking number, payment reference) doesn't require migrations.

**Trade-offs:**  
No ACID transactions across collections. Accepted — this is a portfolio project and we're not doing distributed transactions anyway. Each service owns its own writes.

---

## ADR-002: Sealed interfaces for the event hierarchy

**Status:** Accepted

**Context:**  
Kafka events need a type system. Options: a single generic event class with a `type` string field, an inheritance hierarchy with abstract class, or sealed interfaces with records.

**Decision:**  
Use a sealed interface `OrderEvent` with permitted record implementations: `OrderPlaced`, `PaymentProcessed`, `OrderFulfilled`.

**Reasoning:**  
Sealed interfaces enforce exhaustive handling at compile time. When a new event type is added, the compiler forces every switch expression to handle it — you can't forget. Records give you immutability and zero boilerplate. This is the idiomatic Java 17 approach and demonstrates the cert knowledge concretely.

**Trade-offs:**  
Requires a `type` discriminator field in the serialized JSON so consumers can deserialize to the correct concrete type. Small overhead, worth it.

**Implementation note:**  
The `common` module holds the sealed interface. All services depend on `common`. Do not duplicate event types across services.

---

## ADR-003: Kafka over RabbitMQ

**Status:** Accepted

**Context:**  
Both Kafka and RabbitMQ solve the async messaging problem. RabbitMQ is simpler to configure for small projects.

**Decision:**  
Use Apache Kafka.

**Reasoning:**  
Kafka is the dominant choice in Java backend job descriptions. It demonstrates more relevant industry knowledge. The event log / retention model also maps naturally to the event sourcing pattern we're using (persisting every event to `event_log`). RabbitMQ's message routing model (exchanges, bindings) would add complexity without adding resume value.

**Trade-offs:**  
Kafka requires Zookeeper (or KRaft in newer versions), making the Docker Compose setup heavier. Accepted.

---

## ADR-004: Four separate services over a monolith

**Status:** Accepted

**Context:**  
All the logic could live in one Spring Boot app with internal method calls instead of Kafka messages.

**Decision:**  
Four independent services: Order, Payment, Fulfillment, Notification.

**Reasoning:**  
The point of the project is to demonstrate event-driven architecture. A monolith with Kafka would be contradictory — you'd be adding a message bus to a system that doesn't need one. Separate services justify Kafka's presence and demonstrate understanding of service boundaries.

**Trade-offs:**  
More Docker Compose configuration. More boilerplate per service. Accepted — the architecture is the point.

**Scope boundary:**  
Services communicate only via Kafka topics. No direct HTTP calls between services. No shared database collections.

---

## ADR-005: Manual offset commit in Kafka consumers

**Status:** Accepted

**Context:**  
Kafka consumers can commit offsets automatically (`enable.auto.commit=true`) or manually.

**Decision:**  
Manual offset commit (`enable.auto.commit=false`). Commit only after successful processing and MongoDB write.

**Reasoning:**  
Auto-commit can acknowledge a message before it's been successfully processed. If the service crashes between auto-commit and DB write, the event is lost. Manual commit ensures at-least-once processing — if we crash before committing, the event is redelivered. Combined with idempotency checks in Stage 2, this gives reliable processing.

**Trade-offs:**  
More code. Slightly more complex error handling. The DLQ handles the case where processing fails repeatedly.

---

## ADR-006: No authentication

**Status:** Accepted

**Context:**  
Should the REST API require authentication?

**Decision:**  
No authentication on any endpoint.

**Reasoning:**  
JWT-based auth with Spring Security is already on the resume (Workout Tracker, 40+ endpoints, 3 roles). Adding it here adds code without adding resume differentiation. The project's value is in the event-driven architecture, not the auth layer.

**Trade-offs:**  
Endpoints are open. Acceptable for a local portfolio project running on Docker Compose.

---

## ADR-007: Common module for shared types

**Status:** Accepted

**Context:**  
The sealed event hierarchy needs to be available to all services. Options: duplicate the classes in each service, or extract to a shared module.

**Decision:**  
Create a `common` Maven module. All services declare it as a dependency.

**Reasoning:**  
Duplicating event classes across services means they can diverge — `OrderPlaced` in order-service has different fields than `OrderPlaced` in payment-service. This causes silent deserialization bugs. A single source of truth in `common` prevents this.

**Trade-offs:**  
Tighter coupling between services via the shared module. Accepted — for a portfolio project, this is the right trade-off. In a real microservices environment you'd publish the common module as a versioned artifact to an artifact registry.
