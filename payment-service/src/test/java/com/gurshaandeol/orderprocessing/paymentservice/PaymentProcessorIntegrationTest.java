package com.gurshaandeol.orderprocessing.paymentservice;

import com.gurshaandeol.orderprocessing.common.event.OrderPlaced;
import com.gurshaandeol.orderprocessing.common.model.LineItem;
import com.gurshaandeol.orderprocessing.common.model.ShippingAddress;
import com.gurshaandeol.orderprocessing.paymentservice.model.DeadLetterEntry;
import com.gurshaandeol.orderprocessing.paymentservice.repository.DeadLetterRepository;
import com.gurshaandeol.orderprocessing.paymentservice.repository.EventLogRepository;
import com.gurshaandeol.orderprocessing.paymentservice.service.PaymentProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentProcessorIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private PaymentProcessor paymentProcessor;

    @Autowired
    private EventLogRepository eventLogRepository;

    @Autowired
    private DeadLetterRepository deadLetterRepository;

    @BeforeEach
    void setUp() {
        eventLogRepository.deleteAll();
        deadLetterRepository.deleteAll();
    }

    @Test
    void process_successfulPayment_persistsToEventLog() {
        ReflectionTestUtils.setField(paymentProcessor, "failureRate", 0.0);
        OrderPlaced event = buildOrderPlacedEvent(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        paymentProcessor.process(event);

        assertThat(eventLogRepository.count()).isEqualTo(1);
        assertThat(eventLogRepository.existsByEventId(event.eventId())).isTrue();
        assertThat(deadLetterRepository.count()).isZero();
    }

    @Test
    void process_failedPayment_persistsToDLQ() {
        ReflectionTestUtils.setField(paymentProcessor, "failureRate", 1.0);
        OrderPlaced event = buildOrderPlacedEvent(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        paymentProcessor.process(event);

        assertThat(deadLetterRepository.count()).isEqualTo(1);
        DeadLetterEntry entry = deadLetterRepository.findAll().get(0);
        assertThat(entry.getOriginalEventId()).isEqualTo(event.eventId());
        assertThat(entry.getErrorMessage()).isEqualTo("Simulated payment failure");
        assertThat(entry.getRetryCount()).isZero();

        assertThat(eventLogRepository.count()).isEqualTo(1);
    }

    @Test
    void process_duplicateEvent_isSkipped() {
        ReflectionTestUtils.setField(paymentProcessor, "failureRate", 0.0);
        OrderPlaced event = buildOrderPlacedEvent(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        paymentProcessor.process(event);
        paymentProcessor.process(event);

        assertThat(eventLogRepository.count()).isEqualTo(1);
        assertThat(deadLetterRepository.count()).isZero();
    }

    @Test
    void process_successfulPayment_publishesKafkaEvent() {
        ReflectionTestUtils.setField(paymentProcessor, "failureRate", 0.0);
        String orderId = UUID.randomUUID().toString();
        OrderPlaced event = buildOrderPlacedEvent(UUID.randomUUID().toString(), orderId);

        paymentProcessor.process(event);

        ConsumerRecord<String, String> record = pollTopicForKey("payments.processed", orderId);

        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo(orderId);
        assertThat(record.value()).contains("PaymentProcessed");
    }

    @Test
    void process_failedPayment_publishesToDLQTopic() {
        ReflectionTestUtils.setField(paymentProcessor, "failureRate", 1.0);
        String orderId = UUID.randomUUID().toString();
        OrderPlaced event = buildOrderPlacedEvent(UUID.randomUUID().toString(), orderId);

        paymentProcessor.process(event);

        ConsumerRecord<String, String> record = pollTopicForKey("orders.dlq", orderId);

        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo(orderId);
    }

    private OrderPlaced buildOrderPlacedEvent(String eventId, String orderId) {
        return new OrderPlaced(
                eventId,
                orderId,
                "OrderPlaced",
                Instant.now(),
                "customer-test",
                List.of(new LineItem("prod-1", 1, new BigDecimal("9.99"))),
                new ShippingAddress("1 Test St", "Testville", "T1T 1T1", "CA")
        );
    }
}
