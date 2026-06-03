package com.gurshaandeol.orderprocessing.paymentservice.service;

import com.gurshaandeol.orderprocessing.common.event.OrderPlaced;
import com.gurshaandeol.orderprocessing.common.event.PaymentProcessed;
import com.gurshaandeol.orderprocessing.paymentservice.kafka.PaymentEventPublisher;
import com.gurshaandeol.orderprocessing.paymentservice.model.DeadLetterEntry;
import com.gurshaandeol.orderprocessing.paymentservice.model.EventLogEntry;
import com.gurshaandeol.orderprocessing.paymentservice.repository.DeadLetterRepository;
import com.gurshaandeol.orderprocessing.paymentservice.repository.EventLogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class PaymentProcessor {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessor.class);

    private final EventLogRepository eventLogRepository;
    private final PaymentEventPublisher paymentEventPublisher;
    private final DeadLetterRepository deadLetterRepository;
    private final double failureRate;

    public PaymentProcessor(EventLogRepository eventLogRepository,
                            PaymentEventPublisher paymentEventPublisher,
                            DeadLetterRepository deadLetterRepository,
                            @Value("${payment.failure-rate:0.2}") double failureRate) {
        this.eventLogRepository = eventLogRepository;
        this.paymentEventPublisher = paymentEventPublisher;
        this.deadLetterRepository = deadLetterRepository;
        this.failureRate = failureRate;
    }

    /** Processes an OrderPlaced event. Simulates payment with configurable failure rate. */
    public void process(OrderPlaced event) {
        if (eventLogRepository.existsByEventId(event.eventId())) {
            log.info("Duplicate event detected, skipping: eventId={}", event.eventId());
            return;
        }

        boolean paymentFailed = Math.random() < failureRate;

        EventLogEntry logEntry = new EventLogEntry(
                UUID.randomUUID().toString(),
                event.eventId(),
                "OrderPlaced",
                event.orderId(),
                event,
                Instant.now(),
                "payment-service"
        );
        eventLogRepository.save(logEntry);

        if (!paymentFailed) {
            PaymentProcessed paymentProcessed = new PaymentProcessed(
                    UUID.randomUUID().toString(),
                    event.orderId(),
                    "PaymentProcessed",
                    Instant.now(),
                    UUID.randomUUID().toString(),
                    true
            );
            paymentEventPublisher.publishSuccess(paymentProcessed);
            log.info("Payment succeeded for orderId={}", event.orderId());
        } else {
            paymentEventPublisher.publishFailure(event, "Simulated payment failure");
            DeadLetterEntry entry = new DeadLetterEntry(
                    UUID.randomUUID().toString(),
                    event.eventId(),
                    "OrderPlaced",
                    event,
                    "Simulated payment failure",
                    Instant.now(),
                    0
            );
            deadLetterRepository.save(entry);
            log.warn("Payment failed for orderId={}, routing to DLQ", event.orderId());
            log.warn("Persisted failed event to dead_letter collection, orderId={}", event.orderId());
        }
    }
}
