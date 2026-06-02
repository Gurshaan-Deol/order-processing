package com.gurshaandeol.orderprocessing.paymentservice.kafka;

import com.gurshaandeol.orderprocessing.common.event.OrderPlaced;
import com.gurshaandeol.orderprocessing.common.event.PaymentProcessed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class PaymentEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /** Publishes a successful PaymentProcessed event to payments.processed. */
    public void publishSuccess(PaymentProcessed event) {
        kafkaTemplate.send("payments.processed", event.orderId(), event);
        log.info("Published PaymentProcessed (success) for orderId={}", event.orderId());
    }

    /** Routes a failed payment to the dead letter queue. */
    public void publishFailure(OrderPlaced originalEvent, String errorMessage) {
        PaymentProcessed failed = new PaymentProcessed(
                UUID.randomUUID().toString(),
                originalEvent.orderId(),
                "PaymentProcessed",
                Instant.now(),
                "FAILED-" + UUID.randomUUID(),
                false
        );
        kafkaTemplate.send("orders.dlq", originalEvent.orderId(), failed);
        log.warn("Routed failed payment to DLQ for orderId={}", originalEvent.orderId());
    }
}
