package com.gurshaandeol.orderprocessing.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record ShippingAddress(
        String street,
        String city,
        String postalCode,
        String country
) {
    @JsonCreator
    public ShippingAddress(
            @JsonProperty("street") String street,
            @JsonProperty("city") String city,
            @JsonProperty("postalCode") String postalCode,
            @JsonProperty("country") String country
    ) {
        this.street = street;
        this.city = city;
        this.postalCode = postalCode;
        this.country = country;
    }
}
