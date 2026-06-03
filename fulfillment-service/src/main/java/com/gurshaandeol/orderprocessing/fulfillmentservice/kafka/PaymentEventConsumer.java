package com.gurshaandeol.orderprocessing.fulfillmentservice.kafka;

import com.gurshaandeol.orderprocessing.common.event.PaymentProcessed;
import com.gurshaandeol.orderprocessing.fulfillmentservice.service.FulfillmentProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final FulfillmentProcessor fulfillmentProcessor;

    public PaymentEventConsumer(FulfillmentProcessor fulfillmentProcessor) {
        this.fulfillmentProcessor = fulfillmentProcessor;
    }

    /** Consumes PaymentProcessed events and delegates to FulfillmentProcessor. */
    @KafkaListener(
            topics = "payments.processed",
            groupId = "fulfillment-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, PaymentProcessed> record, Acknowledgment ack) {
        PaymentProcessed event = record.value();
        try {
            fulfillmentProcessor.process(event);
            ack.acknowledge();
        } catch (Exception ex) {
            // Do not acknowledge — Kafka will redeliver so the fulfillment can be retried
            // once the underlying issue (e.g. DB unavailable) is resolved.
            log.error("Failed to process PaymentProcessed event for orderId={}: {}",
                    event.orderId(), ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }
}
