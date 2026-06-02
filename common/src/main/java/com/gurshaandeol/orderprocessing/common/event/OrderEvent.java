package com.gurshaandeol.orderprocessing.common.event;

/** Marker interface for all order domain events. */
public sealed interface OrderEvent permits OrderPlaced, PaymentProcessed, OrderFulfilled {

    String eventId();

    String orderId();

    /** JSON discriminator field used to identify the concrete event type during deserialization. */
    String type();
}
