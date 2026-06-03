package com.gurshaandeol.orderprocessing.orderservice;

import com.gurshaandeol.orderprocessing.common.model.LineItem;
import com.gurshaandeol.orderprocessing.common.model.ShippingAddress;
import com.gurshaandeol.orderprocessing.orderservice.dto.PlaceOrderRequest;
import com.gurshaandeol.orderprocessing.orderservice.model.Order;
import com.gurshaandeol.orderprocessing.orderservice.repository.EventLogRepository;
import com.gurshaandeol.orderprocessing.orderservice.repository.OrderRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OrderServiceIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private EventLogRepository eventLogRepository;

    @BeforeEach
    void setUp() {
        orderRepository.deleteAll();
        eventLogRepository.deleteAll();
    }

    @Test
    void placeOrder_returns201_and_persists_to_mongodb() {
        ResponseEntity<Map> response = restTemplate.postForEntity("/orders", buildPlaceOrderRequest(), Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().get("id")).isNotNull();
        assertThat(response.getBody().get("status")).isEqualTo("PLACED");

        String id = (String) response.getBody().get("id");
        Order saved = orderRepository.findById(id).orElseThrow();
        assertThat(saved.getStatus()).isEqualTo(Order.OrderStatus.PLACED);
        assertThat(saved.getCreatedAt()).isNotNull();
        assertThat(saved.getUpdatedAt()).isNotNull();
    }

    @Test
    void placeOrder_publishes_kafka_event() {
        ResponseEntity<Map> response = restTemplate.postForEntity("/orders", buildPlaceOrderRequest(), Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String orderId = (String) response.getBody().get("id");

        ConsumerRecord<String, String> record = pollTopicForKey("orders.placed", orderId);

        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo(orderId);
        assertThat(record.value()).contains("OrderPlaced");
    }

    @Test
    void getOrder_returns404_for_unknown_id() {
        ResponseEntity<Map> response = restTemplate.getForEntity("/orders/nonexistent-id", Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void getOrder_returns200_with_correct_data() {
        ResponseEntity<Map> created = restTemplate.postForEntity("/orders", buildPlaceOrderRequest(), Map.class);
        String id = (String) created.getBody().get("id");

        ResponseEntity<Map> response = restTemplate.getForEntity("/orders/" + id, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().get("customerId")).isEqualTo("customer-integration-test");
        List<?> lineItems = (List<?>) response.getBody().get("lineItems");
        assertThat(lineItems).hasSize(1);
    }

    @Test
    void placeOrder_rejects_invalid_request_with_400() {
        PlaceOrderRequest invalid = new PlaceOrderRequest();
        invalid.setCustomerId("");
        invalid.setLineItems(List.of());

        ResponseEntity<Map> response = restTemplate.postForEntity("/orders", invalid, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void getOrderStatus_reflects_placed_on_creation() {
        ResponseEntity<Map> created = restTemplate.postForEntity("/orders", buildPlaceOrderRequest(), Map.class);
        String id = (String) created.getBody().get("id");

        ResponseEntity<String> response = restTemplate.getForEntity("/orders/" + id + "/status", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo("PLACED");
    }

    private PlaceOrderRequest buildPlaceOrderRequest() {
        PlaceOrderRequest request = new PlaceOrderRequest();
        request.setCustomerId("customer-integration-test");
        request.setLineItems(List.of(new LineItem("prod-1", 2, new BigDecimal("19.99"))));
        request.setShippingAddress(new ShippingAddress("1 Test St", "Testville", "T1T 1T1", "CA"));
        return request;
    }
}
