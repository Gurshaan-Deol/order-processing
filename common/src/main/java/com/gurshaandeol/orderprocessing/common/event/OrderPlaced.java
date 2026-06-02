package com.gurshaandeol.orderprocessing.common.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.gurshaandeol.orderprocessing.common.model.LineItem;
import com.gurshaandeol.orderprocessing.common.model.ShippingAddress;

import java.time.Instant;
import java.util.List;

public record OrderPlaced(
        String eventId,
        String orderId,
        String type,
        Instant occurredAt,
        String customerId,
        List<LineItem> lineItems,
        ShippingAddress shippingAddress
) implements OrderEvent {

    @JsonCreator
    public OrderPlaced(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("orderId") String orderId,
            @JsonProperty("type") String type,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("customerId") String customerId,
            @JsonProperty("lineItems") List<LineItem> lineItems,
            @JsonProperty("shippingAddress") ShippingAddress shippingAddress
    ) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.type = type;
        this.occurredAt = occurredAt;
        this.customerId = customerId;
        this.lineItems = lineItems;
        this.shippingAddress = shippingAddress;
    }
}
