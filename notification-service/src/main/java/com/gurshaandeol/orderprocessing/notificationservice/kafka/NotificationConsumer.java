package com.gurshaandeol.orderprocessing.notificationservice.kafka;

import com.gurshaandeol.orderprocessing.common.event.OrderEvent;
import com.gurshaandeol.orderprocessing.common.event.OrderFulfilled;
import com.gurshaandeol.orderprocessing.common.event.OrderPlaced;
import com.gurshaandeol.orderprocessing.common.event.PaymentProcessed;
import com.gurshaandeol.orderprocessing.notificationservice.service.NotificationLogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private final NotificationLogger notificationLogger;

    public NotificationConsumer(NotificationLogger notificationLogger) {
        this.notificationLogger = notificationLogger;
    }

    /** Consumes all order events and delegates to NotificationLogger. */
    @KafkaListener(
            topics = {"orders.placed", "payments.processed", "orders.fulfilled"},
            groupId = "notification-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {
        try {
            switch (record.value()) {
                case OrderPlaced event      -> notificationLogger.notifyOrderPlaced(event);
                case PaymentProcessed event -> notificationLogger.notifyPaymentProcessed(event);
                case OrderFulfilled event   -> notificationLogger.notifyOrderFulfilled(event);
                default -> log.warn("Received unknown event type on topic={}, skipping", record.topic());
            }
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Failed to process event on topic={}: {}", record.topic(), ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }
}
