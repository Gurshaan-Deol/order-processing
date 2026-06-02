package com.gurshaandeol.orderprocessing.paymentservice.repository;

import com.gurshaandeol.orderprocessing.paymentservice.model.EventLogEntry;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface EventLogRepository extends MongoRepository<EventLogEntry, String> {

    /** Returns true if an entry with the given eventId already exists (idempotency check). */
    boolean existsByEventId(String eventId);
}
