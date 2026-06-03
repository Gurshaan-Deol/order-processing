package com.gurshaandeol.orderprocessing.fulfillmentservice.service;

import com.gurshaandeol.orderprocessing.common.event.OrderFulfilled;
import com.gurshaandeol.orderprocessing.common.event.PaymentProcessed;
import com.gurshaandeol.orderprocessing.fulfillmentservice.kafka.FulfillmentEventPublisher;
import com.gurshaandeol.orderprocessing.fulfillmentservice.model.EventLogEntry;
import com.gurshaandeol.orderprocessing.fulfillmentservice.repository.EventLogRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FulfillmentProcessorTest {

    @Mock
    private EventLogRepository eventLogRepository;

    @Mock
    private FulfillmentEventPublisher fulfillmentEventPublisher;

    private FulfillmentProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new FulfillmentProcessor(eventLogRepository, fulfillmentEventPublisher);
    }

    @Test
    void process_successfulPayment_publishesOrderFulfilledWithMatchingOrderId() {
        PaymentProcessed event = buildEvent(true);
        when(eventLogRepository.existsByEventId(event.eventId())).thenReturn(false);
        when(eventLogRepository.save(any(EventLogEntry.class))).thenAnswer(inv -> inv.getArgument(0));

        processor.process(event);

        ArgumentCaptor<OrderFulfilled> captor = ArgumentCaptor.forClass(OrderFulfilled.class);
        verify(fulfillmentEventPublisher, times(1)).publish(captor.capture());
        assertThat(captor.getValue().orderId()).isEqualTo(event.orderId());
    }

    @Test
    void process_failedPayment_publishNeverCalled() {
        PaymentProcessed event = buildEvent(false);

        processor.process(event);

        verify(fulfillmentEventPublisher, never()).publish(any());
    }

    @Test
    void process_duplicateEvent_publishNeverCalled() {
        PaymentProcessed event = buildEvent(true);
        when(eventLogRepository.existsByEventId(event.eventId())).thenReturn(true);

        processor.process(event);

        verify(fulfillmentEventPublisher, never()).publish(any());
    }

    @Test
    void process_nonDuplicateSuccessfulPayment_repositorySaveCalledExactlyOnce() {
        PaymentProcessed event = buildEvent(true);
        when(eventLogRepository.existsByEventId(event.eventId())).thenReturn(false);
        when(eventLogRepository.save(any(EventLogEntry.class))).thenAnswer(inv -> inv.getArgument(0));

        processor.process(event);

        verify(eventLogRepository, times(1)).save(any(EventLogEntry.class));
    }

    @Test
    void process_successfulPayment_publishedTrackingNumberStartsWithTRK() {
        PaymentProcessed event = buildEvent(true);
        when(eventLogRepository.existsByEventId(event.eventId())).thenReturn(false);
        when(eventLogRepository.save(any(EventLogEntry.class))).thenAnswer(inv -> inv.getArgument(0));

        processor.process(event);

        ArgumentCaptor<OrderFulfilled> captor = ArgumentCaptor.forClass(OrderFulfilled.class);
        verify(fulfillmentEventPublisher).publish(captor.capture());
        assertThat(captor.getValue().trackingNumber()).startsWith("TRK-");
    }

    private PaymentProcessed buildEvent(boolean success) {
        return new PaymentProcessed(
                "event-" + UUID.randomUUID(),
                "order-id-123",
                "PaymentProcessed",
                Instant.now(),
                "ref-" + UUID.randomUUID(),
                success
        );
    }
}
