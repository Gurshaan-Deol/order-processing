package com.gurshaandeol.orderprocessing.common.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record OrderFulfilled(
        String eventId,
        String orderId,
        String type,
        Instant occurredAt,
        String trackingNumber
) implements OrderEvent {

    @JsonCreator
    public OrderFulfilled(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("orderId") String orderId,
            @JsonProperty("type") String type,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("trackingNumber") String trackingNumber
    ) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.type = type;
        this.occurredAt = occurredAt;
        this.trackingNumber = trackingNumber;
    }
}
