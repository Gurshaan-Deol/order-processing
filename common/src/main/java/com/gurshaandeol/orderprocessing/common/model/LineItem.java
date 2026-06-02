package com.gurshaandeol.orderprocessing.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

public record LineItem(
        String productId,
        int quantity,
        BigDecimal unitPrice
) {
    @JsonCreator
    public LineItem(
            @JsonProperty("productId") String productId,
            @JsonProperty("quantity") int quantity,
            @JsonProperty("unitPrice") BigDecimal unitPrice
    ) {
        this.productId = productId;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
    }
}
