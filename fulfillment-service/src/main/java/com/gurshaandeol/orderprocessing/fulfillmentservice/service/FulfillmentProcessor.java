package com.gurshaandeol.orderprocessing.fulfillmentservice.service;

import com.gurshaandeol.orderprocessing.common.event.OrderFulfilled;
import com.gurshaandeol.orderprocessing.common.event.PaymentProcessed;
import com.gurshaandeol.orderprocessing.fulfillmentservice.kafka.FulfillmentEventPublisher;
import com.gurshaandeol.orderprocessing.fulfillmentservice.model.EventLogEntry;
import com.gurshaandeol.orderprocessing.fulfillmentservice.repository.EventLogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class FulfillmentProcessor {

    private static final Logger log = LoggerFactory.getLogger(FulfillmentProcessor.class);

    private final EventLogRepository eventLogRepository;
    private final FulfillmentEventPublisher fulfillmentEventPublisher;

    public FulfillmentProcessor(EventLogRepository eventLogRepository,
                                FulfillmentEventPublisher fulfillmentEventPublisher) {
        this.eventLogRepository = eventLogRepository;
        this.fulfillmentEventPublisher = fulfillmentEventPublisher;
    }

    /** Processes a PaymentProcessed event and fulfills the order if payment succeeded. */
    public void process(PaymentProcessed event) {
        if (!event.success()) {
            log.info("Skipping fulfillment for failed payment, orderId={}", event.orderId());
            return;
        }

        if (eventLogRepository.existsByEventId(event.eventId())) {
            log.info("Duplicate event detected, skipping: eventId={}", event.eventId());
            return;
        }

        String trackingNumber = "TRK-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        eventLogRepository.save(new EventLogEntry(
                UUID.randomUUID().toString(),
                event.eventId(),
                "PaymentProcessed",
                event.orderId(),
                event,
                Instant.now(),
                "fulfillment-service"
        ));

        OrderFulfilled orderFulfilled = new OrderFulfilled(
                UUID.randomUUID().toString(),
                event.orderId(),
                "OrderFulfilled",
                Instant.now(),
                trackingNumber
        );

        fulfillmentEventPublisher.publish(orderFulfilled);
        log.info("Order fulfilled, orderId={}, trackingNumber={}", event.orderId(), trackingNumber);
    }
}
