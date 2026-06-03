package com.gurshaandeol.orderprocessing.orderservice.dto;

public record MetricsResponse(
        long totalOrders,
        long ordersPlaced,
        long paymentsProcessed,
        long ordersFulfilled,
        long dlqCount,
        long avgPaymentProcessingMs,
        String uptime
) {}
