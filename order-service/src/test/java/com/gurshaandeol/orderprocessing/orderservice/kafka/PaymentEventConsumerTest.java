package com.gurshaandeol.orderprocessing.orderservice.kafka;

import com.gurshaandeol.orderprocessing.common.event.OrderFulfilled;
import com.gurshaandeol.orderprocessing.common.event.PaymentProcessed;
import com.gurshaandeol.orderprocessing.common.model.LineItem;
import com.gurshaandeol.orderprocessing.common.model.ShippingAddress;
import com.gurshaandeol.orderprocessing.orderservice.model.EventLogEntry;
import com.gurshaandeol.orderprocessing.orderservice.model.Order;
import com.gurshaandeol.orderprocessing.orderservice.repository.EventLogRepository;
import com.gurshaandeol.orderprocessing.orderservice.repository.OrderRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentEventConsumerTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private EventLogRepository eventLogRepository;

    @Mock
    private Acknowledgment ack;

    private PaymentEventConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new PaymentEventConsumer(orderRepository, eventLogRepository);
    }

    @Test
    void consume_successfulPayment_updatesStatusToPaymentProcessedAndAcks() {
        PaymentProcessed event = buildPaymentEvent(true);
        ConsumerRecord<String, PaymentProcessed> record = buildPaymentRecord(event);
        Order order = buildOrder(event.orderId());
        when(eventLogRepository.existsByEventId(event.eventId())).thenReturn(false);
        when(orderRepository.findById(event.orderId())).thenReturn(Optional.of(order));
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));
        when(eventLogRepository.save(any(EventLogEntry.class))).thenAnswer(inv -> inv.getArgument(0));

        consumer.consume(record, ack);

        ArgumentCaptor<Order> captor = ArgumentCaptor.forClass(Order.class);
        verify(orderRepository).save(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(Order.OrderStatus.PAYMENT_PROCESSED);
        verify(eventLogRepository, times(1)).save(any(EventLogEntry.class));
        verify(ack, times(1)).acknowledge();
    }

    @Test
    void consume_failedPayment_updatesStatusToFailedAndAcks() {
        PaymentProcessed event = buildPaymentEvent(false);
        ConsumerRecord<String, PaymentProcessed> record = buildPaymentRecord(event);
        Order order = buildOrder(event.orderId());
        when(eventLogRepository.existsByEventId(event.eventId())).thenReturn(false);
        when(orderRepository.findById(event.orderId())).thenReturn(Optional.of(order));
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));
        when(eventLogRepository.save(any(EventLogEntry.class))).thenAnswer(inv -> inv.getArgument(0));

        consumer.consume(record, ack);

        ArgumentCaptor<Order> captor = ArgumentCaptor.forClass(Order.class);
        verify(orderRepository).save(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(Order.OrderStatus.FAILED);
        verify(eventLogRepository, times(1)).save(any(EventLogEntry.class));
        verify(ack, times(1)).acknowledge();
    }

    @Test
    void consume_orderNotFound_acksWithoutSaving() {
        PaymentProcessed event = buildPaymentEvent(true);
        ConsumerRecord<String, PaymentProcessed> record = buildPaymentRecord(event);
        when(eventLogRepository.existsByEventId(event.eventId())).thenReturn(false);
        when(orderRepository.findById(event.orderId())).thenReturn(Optional.empty());

        consumer.consume(record, ack);

        verify(orderRepository, never()).save(any());
        verify(eventLogRepository, never()).save(any());
        verify(ack, times(1)).acknowledge();
    }

    @Test
    void consume_saveThrows_doesNotAcknowledge() {
        PaymentProcessed event = buildPaymentEvent(true);
        ConsumerRecord<String, PaymentProcessed> record = buildPaymentRecord(event);
        Order order = buildOrder(event.orderId());
        when(eventLogRepository.existsByEventId(event.eventId())).thenReturn(false);
        when(orderRepository.findById(event.orderId())).thenReturn(Optional.of(order));
        when(orderRepository.save(any(Order.class))).thenThrow(new RuntimeException("DB unavailable"));

        assertThrows(RuntimeException.class, () -> consumer.consume(record, ack));

        verify(ack, never()).acknowledge();
    }

    @Test
    void handlePaymentProcessed_duplicateEvent_skipsProcessingAndAcks() {
        PaymentProcessed event = buildPaymentEvent(true);
        ConsumerRecord<String, PaymentProcessed> record = buildPaymentRecord(event);
        when(eventLogRepository.existsByEventId(event.eventId())).thenReturn(true);

        consumer.consume(record, ack);

        verify(orderRepository, never()).save(any());
        verify(ack, times(1)).acknowledge();
    }

    @Test
    void handleOrderFulfilled_duplicateEvent_skipsProcessingAndAcks() {
        OrderFulfilled event = buildFulfilledEvent();
        ConsumerRecord<String, OrderFulfilled> record = buildFulfilledRecord(event);
        when(eventLogRepository.existsByEventId(event.eventId())).thenReturn(true);

        consumer.consume(record, ack);

        verify(orderRepository, never()).save(any());
        verify(ack, times(1)).acknowledge();
    }

    private PaymentProcessed buildPaymentEvent(boolean success) {
        return new PaymentProcessed(
                "event-" + UUID.randomUUID(),
                "order-id-123",
                "PaymentProcessed",
                Instant.now(),
                "ref-" + UUID.randomUUID(),
                success
        );
    }

    private OrderFulfilled buildFulfilledEvent() {
        return new OrderFulfilled(
                "event-" + UUID.randomUUID(),
                "order-id-123",
                "OrderFulfilled",
                Instant.now(),
                "TRK-" + UUID.randomUUID()
        );
    }

    private ConsumerRecord<String, PaymentProcessed> buildPaymentRecord(PaymentProcessed event) {
        return new ConsumerRecord<>("payments.processed", 0, 0L, event.orderId(), event);
    }

    private ConsumerRecord<String, OrderFulfilled> buildFulfilledRecord(OrderFulfilled event) {
        return new ConsumerRecord<>("orders.fulfilled", 0, 0L, event.orderId(), event);
    }

    private Order buildOrder(String orderId) {
        Order order = new Order();
        order.setId(orderId);
        order.setCustomerId("customer-001");
        order.setStatus(Order.OrderStatus.PLACED);
        order.setLineItems(List.of(new LineItem("prod-1", 1, new BigDecimal("9.99"))));
        order.setShippingAddress(new ShippingAddress("1 Main St", "Toronto", "M1A1A1", "Canada"));
        order.setCreatedAt(Instant.now());
        order.setUpdatedAt(Instant.now());
        return order;
    }
}
