package com.gurshaandeol.orderprocessing.orderservice.service;

import com.gurshaandeol.orderprocessing.common.event.OrderPlaced;
import com.gurshaandeol.orderprocessing.orderservice.dto.OrderResponse;
import com.gurshaandeol.orderprocessing.orderservice.dto.PlaceOrderRequest;
import com.gurshaandeol.orderprocessing.orderservice.kafka.OrderEventPublisher;
import com.gurshaandeol.orderprocessing.orderservice.model.Order;
import com.gurshaandeol.orderprocessing.orderservice.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OrderEventPublisher orderEventPublisher;

    public OrderService(OrderRepository orderRepository, OrderEventPublisher orderEventPublisher) {
        this.orderRepository = orderRepository;
        this.orderEventPublisher = orderEventPublisher;
    }

    /** Creates a new order, persists it, publishes an OrderPlaced event, and returns the mapped response. */
    public OrderResponse placeOrder(PlaceOrderRequest request) {
        Order order = new Order();
        order.setId(UUID.randomUUID().toString());
        order.setCustomerId(request.getCustomerId());
        order.setStatus(Order.OrderStatus.PLACED);
        order.setLineItems(request.getLineItems());
        order.setShippingAddress(request.getShippingAddress());
        order.setCreatedAt(Instant.now());
        order.setUpdatedAt(Instant.now());

        Order saved = orderRepository.save(order);

        OrderPlaced event = new OrderPlaced(
                UUID.randomUUID().toString(),
                saved.getId(),
                "OrderPlaced",
                Instant.now(),
                saved.getCustomerId(),
                saved.getLineItems(),
                saved.getShippingAddress()
        );

        try {
            orderEventPublisher.publish(event);
        } catch (Exception e) {
            log.error("Failed to publish OrderPlaced event for orderId={}", saved.getId(), e);
            throw e;
        }

        return toResponse(saved);
    }

    /** Fetches an order by id and maps it to a response, or returns empty if not found. */
    public Optional<OrderResponse> getOrder(String id) {
        return orderRepository.findById(id).map(this::toResponse);
    }

    private OrderResponse toResponse(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getStatus().name(),
                order.getLineItems(),
                order.getShippingAddress(),
                order.getTrackingNumber(),
                order.getCreatedAt(),
                order.getUpdatedAt()
        );
    }
}
