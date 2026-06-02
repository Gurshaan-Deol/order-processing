package com.gurshaandeol.orderprocessing.orderservice.dto;

import com.gurshaandeol.orderprocessing.common.model.LineItem;
import com.gurshaandeol.orderprocessing.common.model.ShippingAddress;

import java.time.Instant;
import java.util.List;

public record OrderResponse(
        String id,
        String customerId,
        String status,
        List<LineItem> lineItems,
        ShippingAddress shippingAddress,
        String trackingNumber,
        Instant createdAt,
        Instant updatedAt
) {}
