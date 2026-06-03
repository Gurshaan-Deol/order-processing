package com.gurshaandeol.orderprocessing.orderservice.service;

import com.gurshaandeol.orderprocessing.orderservice.dto.MetricsResponse;
import com.gurshaandeol.orderprocessing.orderservice.model.Order;
import com.gurshaandeol.orderprocessing.orderservice.repository.OrderRepository;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.mongodb.core.MongoTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@SuppressWarnings("unchecked")
class MetricsServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private MongoTemplate mongoTemplate;

    @Mock
    private MongoCollection<Document> deadLetterCollection;

    @Mock
    private MongoCollection<Document> eventLogCollection;

    @Mock
    private FindIterable<Document> findIterable;

    @Mock
    private MongoCursor<Document> cursor;

    private MetricsService metricsService;

    @BeforeEach
    void setUp() {
        metricsService = new MetricsService(orderRepository, mongoTemplate);
        when(mongoTemplate.getCollection("dead_letter")).thenReturn(deadLetterCollection);
        when(deadLetterCollection.countDocuments()).thenReturn(0L);
        when(mongoTemplate.getCollection("event_log")).thenReturn(eventLogCollection);
        when(eventLogCollection.find()).thenReturn(findIterable);
        when(findIterable.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(false);
    }

    @Test
    void getMetrics_fiveFullyFulfilledOrders_totalOrdersAndOrdersFulfilledAreFive() {
        when(orderRepository.count()).thenReturn(5L);
        when(orderRepository.countByStatus(Order.OrderStatus.FULFILLED)).thenReturn(5L);

        MetricsResponse metrics = metricsService.getMetrics();

        assertThat(metrics.totalOrders()).isEqualTo(5L);
        assertThat(metrics.ordersFulfilled()).isEqualTo(5L);
    }

    @Test
    void getMetrics_emptyRepository_dlqCountIsZeroAndAvgProcessingMsIsZero() {
        MetricsResponse metrics = metricsService.getMetrics();

        assertThat(metrics.dlqCount()).isEqualTo(0L);
        assertThat(metrics.avgPaymentProcessingMs()).isEqualTo(0L);
    }

    @Test
    void getMetrics_uptimeIsNonNullAndNonEmpty() {
        MetricsResponse metrics = metricsService.getMetrics();

        assertThat(metrics.uptime()).isNotNull().isNotEmpty();
    }
}
