package com.gurshaandeol.orderprocessing.notificationservice.service;

import com.gurshaandeol.orderprocessing.common.event.OrderFulfilled;
import com.gurshaandeol.orderprocessing.common.event.OrderPlaced;
import com.gurshaandeol.orderprocessing.common.event.PaymentProcessed;
import com.gurshaandeol.orderprocessing.common.model.LineItem;
import com.gurshaandeol.orderprocessing.common.model.ShippingAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatNoException;

@ExtendWith(MockitoExtension.class)
class NotificationLoggerTest {

    private NotificationLogger notificationLogger;

    @BeforeEach
    void setUp() {
        notificationLogger = new NotificationLogger();
    }

    @Test
    void notifyOrderPlaced_validEventWithTwoLineItems_doesNotThrow() {
        OrderPlaced event = buildOrderPlaced();
        assertThatNoException().isThrownBy(() -> notificationLogger.notifyOrderPlaced(event));
    }

    @Test
    void notifyPaymentProcessed_successfulPayment_doesNotThrow() {
        PaymentProcessed event = buildPaymentProcessed(true);
        assertThatNoException().isThrownBy(() -> notificationLogger.notifyPaymentProcessed(event));
    }

    @Test
    void notifyPaymentProcessed_failedPayment_doesNotThrow() {
        PaymentProcessed event = buildPaymentProcessed(false);
        assertThatNoException().isThrownBy(() -> notificationLogger.notifyPaymentProcessed(event));
    }

    @Test
    void notifyOrderFulfilled_validEventWithTrackingNumber_doesNotThrow() {
        OrderFulfilled event = buildOrderFulfilled();
        assertThatNoException().isThrownBy(() -> notificationLogger.notifyOrderFulfilled(event));
    }

    private OrderPlaced buildOrderPlaced() {
        return new OrderPlaced(
                "event-" + UUID.randomUUID(),
                "order-" + UUID.randomUUID(),
                "OrderPlaced",
                Instant.now(),
                "customer-001",
                List.of(
                        new LineItem("prod-abc", 2, new BigDecimal("29.99")),
                        new LineItem("prod-xyz", 1, new BigDecimal("49.99"))
                ),
                new ShippingAddress("123 Main St", "Toronto", "M1A 1A1", "Canada")
        );
    }

    private PaymentProcessed buildPaymentProcessed(boolean success) {
        return new PaymentProcessed(
                "event-" + UUID.randomUUID(),
                "order-" + UUID.randomUUID(),
                "PaymentProcessed",
                Instant.now(),
                "ref-" + UUID.randomUUID(),
                success
        );
    }

    private OrderFulfilled buildOrderFulfilled() {
        return new OrderFulfilled(
                "event-" + UUID.randomUUID(),
                "order-" + UUID.randomUUID(),
                "OrderFulfilled",
                Instant.now(),
                "TRK-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase()
        );
    }
}
