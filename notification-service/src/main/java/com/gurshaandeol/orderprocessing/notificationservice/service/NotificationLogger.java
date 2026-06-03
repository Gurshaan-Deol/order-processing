package com.gurshaandeol.orderprocessing.notificationservice.service;

import com.gurshaandeol.orderprocessing.common.event.OrderFulfilled;
import com.gurshaandeol.orderprocessing.common.event.OrderPlaced;
import com.gurshaandeol.orderprocessing.common.event.PaymentProcessed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class NotificationLogger {

    private static final Logger log = LoggerFactory.getLogger(NotificationLogger.class);

    /** Logs a structured notification for a placed order. */
    public void notifyOrderPlaced(OrderPlaced event) {
        log.info("NOTIFICATION [ORDER_PLACED] orderId={} customerId={} items={} occurredAt={}",
                event.orderId(), event.customerId(), event.lineItems().size(), event.occurredAt());
    }

    /** Logs a structured notification for a processed payment. */
    public void notifyPaymentProcessed(PaymentProcessed event) {
        log.info("NOTIFICATION [PAYMENT_PROCESSED] orderId={} success={} reference={} occurredAt={}",
                event.orderId(), event.success(), event.paymentReference(), event.occurredAt());
    }

    /** Logs a structured notification for a fulfilled order. */
    public void notifyOrderFulfilled(OrderFulfilled event) {
        log.info("NOTIFICATION [ORDER_FULFILLED] orderId={} trackingNumber={} occurredAt={}",
                event.orderId(), event.trackingNumber(), event.occurredAt());
    }
}
