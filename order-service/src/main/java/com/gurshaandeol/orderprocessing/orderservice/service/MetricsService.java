package com.gurshaandeol.orderprocessing.orderservice.service;

import com.gurshaandeol.orderprocessing.orderservice.dto.MetricsResponse;
import com.gurshaandeol.orderprocessing.orderservice.model.Order;
import com.gurshaandeol.orderprocessing.orderservice.repository.OrderRepository;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Service
public class MetricsService {

    private static final Logger log = LoggerFactory.getLogger(MetricsService.class);

    private final OrderRepository orderRepository;
    private final MongoTemplate mongoTemplate;

    public MetricsService(OrderRepository orderRepository, MongoTemplate mongoTemplate) {
        this.orderRepository = orderRepository;
        this.mongoTemplate = mongoTemplate;
    }

    /** Computes and returns current system metrics. */
    public MetricsResponse getMetrics() {
        long totalOrders = orderRepository.count();
        long ordersPlaced = totalOrders;
        long paymentsProcessed = orderRepository.countByStatus(Order.OrderStatus.PAYMENT_PROCESSED)
                + orderRepository.countByStatus(Order.OrderStatus.FULFILLED)
                + orderRepository.countByStatus(Order.OrderStatus.FAILED);
        long ordersFulfilled = orderRepository.countByStatus(Order.OrderStatus.FULFILLED);
        long dlqCount = mongoTemplate.getCollection("dead_letter").countDocuments();
        long avgPaymentProcessingMs = computeAvgPaymentProcessingMs();
        String uptime = formatUptime(ManagementFactory.getRuntimeMXBean().getUptime());

        return new MetricsResponse(
                totalOrders,
                ordersPlaced,
                paymentsProcessed,
                ordersFulfilled,
                dlqCount,
                avgPaymentProcessingMs,
                uptime
        );
    }

    private long computeAvgPaymentProcessingMs() {
        List<Long> processingTimes = new ArrayList<>();

        try (MongoCursor<Document> cursor = mongoTemplate.getCollection("event_log").find().iterator()) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                if (!"payment-service".equals(doc.getString("serviceId"))) {
                    continue;
                }
                String orderId = doc.getString("orderId");
                Date processedAtDate = doc.getDate("processedAt");
                if (orderId == null || processedAtDate == null) {
                    continue;
                }
                orderRepository.findById(orderId).ifPresent(order -> {
                    long ms = Duration.between(order.getCreatedAt(), processedAtDate.toInstant()).toMillis();
                    if (ms >= 0) {
                        processingTimes.add(ms);
                    }
                });
            }
        }

        if (processingTimes.isEmpty()) {
            return 0L;
        }
        return processingTimes.stream().mapToLong(Long::longValue).sum() / processingTimes.size();
    }

    private String formatUptime(long uptimeMs) {
        long totalSeconds = uptimeMs / 1000;
        long hours = totalSeconds / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        if (hours > 0) {
            return hours + "h " + minutes + "m";
        }
        return minutes + "m " + seconds + "s";
    }
}
