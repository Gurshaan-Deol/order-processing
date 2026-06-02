package com.gurshaandeol.orderprocessing.paymentservice.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.gurshaandeol.orderprocessing.common.event.OrderPlaced;
import com.gurshaandeol.orderprocessing.paymentservice.service.PaymentProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final PaymentProcessor paymentProcessor;
    private final PaymentEventPublisher paymentEventPublisher;

    public OrderEventConsumer(PaymentProcessor paymentProcessor,
                              PaymentEventPublisher paymentEventPublisher) {
        this.paymentProcessor = paymentProcessor;
        this.paymentEventPublisher = paymentEventPublisher;
    }

    /** Consumes OrderPlaced events and delegates to PaymentProcessor. */
    @KafkaListener(
            topics = "orders.placed",
            groupId = "payment-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, OrderPlaced> record, Acknowledgment ack) {
        OrderPlaced event = record.value();
        try {
            paymentProcessor.process(event);
            ack.acknowledge();
        } catch (Exception ex) {
            if (ex instanceof JsonProcessingException jpEx) {
                // Deserialization failed — the message is malformed and will never succeed.
                // Route to DLQ and commit the offset so we do not block the partition.
                log.error("Deserialization error for message at offset={}, routing to DLQ",
                        record.offset(), jpEx);
                OrderPlaced placeholder = new OrderPlaced(
                        UUID.randomUUID().toString(),
                        record.key() != null ? record.key() : "unknown",
                        "OrderPlaced",
                        Instant.now(),
                        "unknown",
                        List.of(),
                        null
                );
                paymentEventPublisher.publishFailure(placeholder, jpEx.getMessage());
                ack.acknowledge();
            } else {
                // Unexpected failure (e.g. DB unavailable). Do not acknowledge — Kafka will
                // redeliver the message. A DeadLetterPublishingRecoverer with BackOff should
                // be configured in production to cap the retry count.
                log.error("Unexpected error processing event orderId={}: {}",
                        event != null ? event.orderId() : "unknown", ex.getMessage(), ex);
                throw new RuntimeException("Payment processing failed for orderId="
                        + (event != null ? event.orderId() : "unknown"), ex);
            }
        }
    }
}
