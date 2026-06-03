package com.gurshaandeol.orderprocessing.fulfillmentservice;

import com.gurshaandeol.orderprocessing.common.event.PaymentProcessed;
import com.gurshaandeol.orderprocessing.fulfillmentservice.repository.EventLogRepository;
import com.gurshaandeol.orderprocessing.fulfillmentservice.service.FulfillmentProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class FulfillmentProcessorIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private FulfillmentProcessor fulfillmentProcessor;

    @Autowired
    private EventLogRepository eventLogRepository;

    @BeforeEach
    void setUp() {
        eventLogRepository.deleteAll();
    }

    @Test
    void process_successfulPayment_persistsToEventLog() {
        PaymentProcessed event = buildPaymentProcessedEvent(true);

        fulfillmentProcessor.process(event);

        assertThat(eventLogRepository.count()).isEqualTo(1);
        assertThat(eventLogRepository.existsByEventId(event.eventId())).isTrue();
        assertThat(eventLogRepository.findAll().get(0).getServiceId()).isEqualTo("fulfillment-service");
    }

    @Test
    void process_failedPayment_isSkipped() {
        PaymentProcessed event = buildPaymentProcessedEvent(false);

        fulfillmentProcessor.process(event);

        assertThat(eventLogRepository.count()).isZero();
    }

    @Test
    void process_duplicateEvent_isSkipped() {
        PaymentProcessed event = buildPaymentProcessedEvent(true);

        fulfillmentProcessor.process(event);
        fulfillmentProcessor.process(event);

        assertThat(eventLogRepository.count()).isEqualTo(1);
    }

    @Test
    void process_successfulPayment_publishesKafkaEvent() {
        PaymentProcessed event = buildPaymentProcessedEvent(true);

        fulfillmentProcessor.process(event);

        ConsumerRecord<String, String> record = pollTopic("orders.fulfilled");

        assertThat(record).isNotNull();
        assertThat(record.value()).contains("OrderFulfilled");
        assertThat(record.value()).contains("TRK-");
    }

    @Test
    void process_trackingNumber_hasCorrectFormat() {
        PaymentProcessed event = buildPaymentProcessedEvent(true);

        fulfillmentProcessor.process(event);

        ConsumerRecord<String, String> record = pollTopic("orders.fulfilled");
        assertThat(record).isNotNull();

        Matcher matcher = Pattern.compile("\"trackingNumber\":\"([^\"]+)\"").matcher(record.value());
        assertThat(matcher.find()).isTrue();
        String trackingNumber = matcher.group(1);
        assertThat(trackingNumber).startsWith("TRK-");
        assertThat(trackingNumber).hasSize(12);
    }

    private PaymentProcessed buildPaymentProcessedEvent(boolean success) {
        return new PaymentProcessed(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                "PaymentProcessed",
                Instant.now(),
                UUID.randomUUID().toString(),
                success
        );
    }
}
