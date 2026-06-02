package com.gurshaandeol.orderprocessing.common.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record PaymentProcessed(
        String eventId,
        String orderId,
        String type,
        Instant occurredAt,
        String paymentReference,
        boolean success
) implements OrderEvent {

    @JsonCreator
    public PaymentProcessed(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("orderId") String orderId,
            @JsonProperty("type") String type,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("paymentReference") String paymentReference,
            @JsonProperty("success") boolean success
    ) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.type = type;
        this.occurredAt = occurredAt;
        this.paymentReference = paymentReference;
        this.success = success;
    }
}
