package com.gurshaandeol.orderprocessing.orderservice.kafka;

import com.gurshaandeol.orderprocessing.common.event.OrderFulfilled;
import com.gurshaandeol.orderprocessing.common.event.PaymentProcessed;
import com.gurshaandeol.orderprocessing.orderservice.model.EventLogEntry;
import com.gurshaandeol.orderprocessing.orderservice.model.Order;
import com.gurshaandeol.orderprocessing.orderservice.repository.EventLogRepository;
import com.gurshaandeol.orderprocessing.orderservice.repository.OrderRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Component
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final OrderRepository orderRepository;
    private final EventLogRepository eventLogRepository;

    public PaymentEventConsumer(OrderRepository orderRepository, EventLogRepository eventLogRepository) {
        this.orderRepository = orderRepository;
        this.eventLogRepository = eventLogRepository;
    }

    /** Consumes PaymentProcessed and OrderFulfilled events and updates order status in MongoDB. */
    @KafkaListener(
            topics = {"payments.processed", "orders.fulfilled"},
            groupId = "order-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, ?> record, Acknowledgment ack) {
        try {
            switch (record.value()) {
                case PaymentProcessed event -> handlePaymentProcessed(event, ack);
                case OrderFulfilled event   -> handleOrderFulfilled(event, ack);
                default -> {
                    log.warn("Received unknown event type on topic {}, acknowledging and skipping",
                            record.topic());
                    ack.acknowledge();
                }
            }
        } catch (Exception ex) {
            // Do not acknowledge — Kafka will redeliver this message so the status update
            // can be retried once the underlying issue (e.g. DB unavailable) is resolved.
            log.error("Failed to process event from topic={} key={}: {}",
                    record.topic(), record.key(), ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    private void handlePaymentProcessed(PaymentProcessed event, Acknowledgment ack) {
        if (eventLogRepository.existsByEventId(event.eventId())) {
            log.info("Duplicate event detected, skipping: eventId={}", event.eventId());
            ack.acknowledge();
            return;
        }

        Optional<Order> found = orderRepository.findById(event.orderId());
        if (found.isEmpty()) {
            log.warn("Received PaymentProcessed for unknown orderId={}, acknowledging and skipping",
                    event.orderId());
            ack.acknowledge();
            return;
        }

        Order order = found.get();
        if (event.success()) {
            order.setStatus(Order.OrderStatus.PAYMENT_PROCESSED);
            order.setUpdatedAt(Instant.now());
            orderRepository.save(order);
            log.info("Order status updated to PAYMENT_PROCESSED for orderId={}", event.orderId());
        } else {
            order.setStatus(Order.OrderStatus.FAILED);
            order.setUpdatedAt(Instant.now());
            orderRepository.save(order);
            log.warn("Order status updated to FAILED for orderId={}", event.orderId());
        }

        eventLogRepository.save(new EventLogEntry(
                UUID.randomUUID().toString(),
                event.eventId(),
                event.type(),
                event.orderId(),
                event,
                Instant.now(),
                "order-service"
        ));
        ack.acknowledge();
    }

    private void handleOrderFulfilled(OrderFulfilled event, Acknowledgment ack) {
        if (eventLogRepository.existsByEventId(event.eventId())) {
            log.info("Duplicate event detected, skipping: eventId={}", event.eventId());
            ack.acknowledge();
            return;
        }

        Optional<Order> found = orderRepository.findById(event.orderId());
        if (found.isEmpty()) {
            log.warn("Received OrderFulfilled for unknown orderId={}, acknowledging and skipping",
                    event.orderId());
            ack.acknowledge();
            return;
        }

        Order order = found.get();
        order.setStatus(Order.OrderStatus.FULFILLED);
        order.setTrackingNumber(event.trackingNumber());
        order.setUpdatedAt(Instant.now());
        orderRepository.save(order);

        eventLogRepository.save(new EventLogEntry(
                UUID.randomUUID().toString(),
                event.eventId(),
                event.type(),
                event.orderId(),
                event,
                Instant.now(),
                "order-service"
        ));
        ack.acknowledge();
        log.info("Order status updated to FULFILLED for orderId={}, trackingNumber={}",
                event.orderId(), event.trackingNumber());
    }
}
