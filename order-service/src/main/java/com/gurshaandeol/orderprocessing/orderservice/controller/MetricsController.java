package com.gurshaandeol.orderprocessing.orderservice.controller;

import com.gurshaandeol.orderprocessing.orderservice.dto.MetricsResponse;
import com.gurshaandeol.orderprocessing.orderservice.service.MetricsService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MetricsController {

    private final MetricsService metricsService;

    public MetricsController(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    /** Returns live system metrics computed from MongoDB. */
    @GetMapping("/metrics")
    @ResponseStatus(HttpStatus.OK)
    public MetricsResponse getMetrics() {
        return metricsService.getMetrics();
    }
}
