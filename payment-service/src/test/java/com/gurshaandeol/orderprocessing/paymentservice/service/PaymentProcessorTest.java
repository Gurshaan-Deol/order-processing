package com.gurshaandeol.orderprocessing.paymentservice.service;

import com.gurshaandeol.orderprocessing.common.event.OrderPlaced;
import com.gurshaandeol.orderprocessing.common.event.PaymentProcessed;
import com.gurshaandeol.orderprocessing.common.model.LineItem;
import com.gurshaandeol.orderprocessing.common.model.ShippingAddress;
import com.gurshaandeol.orderprocessing.paymentservice.kafka.PaymentEventPublisher;
import com.gurshaandeol.orderprocessing.paymentservice.model.EventLogEntry;
import com.gurshaandeol.orderprocessing.paymentservice.repository.EventLogRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentProcessorTest {

    @Mock
    private EventLogRepository eventLogRepository;

    @Mock
    private PaymentEventPublisher paymentEventPublisher;

    @Test
    void process_paymentSucceeds_publishSuccessCalledOnce() {
        PaymentProcessor processor = new PaymentProcessor(eventLogRepository, paymentEventPublisher, 0.0);
        OrderPlaced event = buildEvent();
        when(eventLogRepository.existsByEventId(event.eventId())).thenReturn(false);
        when(eventLogRepository.save(any(EventLogEntry.class))).thenAnswer(inv -> inv.getArgument(0));

        processor.process(event);

        verify(paymentEventPublisher, times(1)).publishSuccess(any(PaymentProcessed.class));
        verify(paymentEventPublisher, never()).publishFailure(any(OrderPlaced.class), anyString());
    }

    @Test
    void process_paymentFails_publishFailureCalledAndSuccessNeverCalled() {
        PaymentProcessor processor = new PaymentProcessor(eventLogRepository, paymentEventPublisher, 1.0);
        OrderPlaced event = buildEvent();
        when(eventLogRepository.existsByEventId(event.eventId())).thenReturn(false);
        when(eventLogRepository.save(any(EventLogEntry.class))).thenAnswer(inv -> inv.getArgument(0));

        processor.process(event);

        verify(paymentEventPublisher, times(1)).publishFailure(any(OrderPlaced.class), anyString());
        verify(paymentEventPublisher, never()).publishSuccess(any(PaymentProcessed.class));
    }

    @Test
    void process_duplicateEvent_neitherPublishMethodCalled() {
        PaymentProcessor processor = new PaymentProcessor(eventLogRepository, paymentEventPublisher, 0.0);
        OrderPlaced event = buildEvent();
        when(eventLogRepository.existsByEventId(event.eventId())).thenReturn(true);

        processor.process(event);

        verify(paymentEventPublisher, never()).publishSuccess(any());
        verify(paymentEventPublisher, never()).publishFailure(any(), anyString());
    }

    @Test
    void process_nonDuplicateEvent_repositorySaveCalledExactlyOnce() {
        PaymentProcessor processor = new PaymentProcessor(eventLogRepository, paymentEventPublisher, 0.0);
        OrderPlaced event = buildEvent();
        when(eventLogRepository.existsByEventId(event.eventId())).thenReturn(false);
        when(eventLogRepository.save(any(EventLogEntry.class))).thenAnswer(inv -> inv.getArgument(0));

        processor.process(event);

        verify(eventLogRepository, times(1)).save(any(EventLogEntry.class));
    }

    private OrderPlaced buildEvent() {
        return new OrderPlaced(
                "test-event-id",
                "test-order-id",
                "OrderPlaced",
                Instant.now(),
                "test-customer",
                List.of(new LineItem("prod-1", 1, new BigDecimal("9.99"))),
                new ShippingAddress("1 Main St", "Springfield", "12345", "US")
        );
    }
}
