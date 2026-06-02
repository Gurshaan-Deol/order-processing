package com.gurshaandeol.orderprocessing.orderservice.service;

import com.gurshaandeol.orderprocessing.common.event.OrderPlaced;
import com.gurshaandeol.orderprocessing.common.model.LineItem;
import com.gurshaandeol.orderprocessing.common.model.ShippingAddress;
import com.gurshaandeol.orderprocessing.orderservice.dto.OrderResponse;
import com.gurshaandeol.orderprocessing.orderservice.dto.PlaceOrderRequest;
import com.gurshaandeol.orderprocessing.orderservice.kafka.OrderEventPublisher;
import com.gurshaandeol.orderprocessing.orderservice.model.Order;
import com.gurshaandeol.orderprocessing.orderservice.repository.OrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OrderEventPublisher orderEventPublisher;

    private OrderService orderService;

    @BeforeEach
    void setUp() {
        orderService = new OrderService(orderRepository, orderEventPublisher);
    }

    @Test
    void placeOrder_savedOrderHasStatusPlacedAndNonNullIdAndTimestamps() {
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

        ArgumentCaptor<Order> captor = ArgumentCaptor.forClass(Order.class);
        orderService.placeOrder(buildRequest());
        verify(orderRepository).save(captor.capture());

        Order saved = captor.getValue();
        assertThat(saved.getStatus()).isEqualTo(Order.OrderStatus.PLACED);
        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getCreatedAt()).isNotNull();
        assertThat(saved.getUpdatedAt()).isNotNull();
    }

    @Test
    void placeOrder_repositorySaveCalledExactlyOnce() {
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

        orderService.placeOrder(buildRequest());

        verify(orderRepository, times(1)).save(any(Order.class));
    }

    @Test
    void getOrder_returnsEmptyWhenRepositoryReturnsEmpty() {
        when(orderRepository.findById("nonexistent")).thenReturn(Optional.empty());

        Optional<OrderResponse> result = orderService.getOrder("nonexistent");

        assertThat(result).isEmpty();
    }

    @Test
    void getOrder_returnsMappedResponseWhenFound() {
        Order order = new Order();
        order.setId("order-123");
        order.setCustomerId("customer-1");
        order.setStatus(Order.OrderStatus.PLACED);
        order.setLineItems(List.of(new LineItem("prod-1", 1, new BigDecimal("9.99"))));
        order.setShippingAddress(new ShippingAddress("1 Main St", "Springfield", "12345", "US"));
        order.setCreatedAt(Instant.now());
        order.setUpdatedAt(Instant.now());
        when(orderRepository.findById("order-123")).thenReturn(Optional.of(order));

        Optional<OrderResponse> result = orderService.getOrder("order-123");

        assertThat(result).isPresent();
        assertThat(result.get().id()).isEqualTo("order-123");
        assertThat(result.get().customerId()).isEqualTo("customer-1");
        assertThat(result.get().status()).isEqualTo("PLACED");
    }

    @Test
    void placeOrder_publishesOrderPlacedEventExactlyOnce() {
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

        orderService.placeOrder(buildRequest());

        verify(orderEventPublisher, times(1)).publish(any(OrderPlaced.class));
    }

    private PlaceOrderRequest buildRequest() {
        PlaceOrderRequest request = new PlaceOrderRequest();
        request.setCustomerId("customer-1");
        request.setLineItems(List.of(new LineItem("prod-1", 2, new BigDecimal("9.99"))));
        request.setShippingAddress(new ShippingAddress("123 Main St", "Springfield", "12345", "US"));
        return request;
    }
}
