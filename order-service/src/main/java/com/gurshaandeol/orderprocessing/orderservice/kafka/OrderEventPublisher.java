package com.gurshaandeol.orderprocessing.orderservice.kafka;

import com.gurshaandeol.orderprocessing.common.event.OrderPlaced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /** Publishes an OrderPlaced event to the orders.placed topic. */
    public void publish(OrderPlaced event) {
        kafkaTemplate.send("orders.placed", event.orderId(), event);
        log.info("Published OrderPlaced event for orderId={}", event.orderId());
    }
}
