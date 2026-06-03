package com.gurshaandeol.orderprocessing.fulfillmentservice.kafka;

import com.gurshaandeol.orderprocessing.common.event.OrderFulfilled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class FulfillmentEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(FulfillmentEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public FulfillmentEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /** Publishes an OrderFulfilled event to the orders.fulfilled topic. */
    public void publish(OrderFulfilled event) {
        kafkaTemplate.send("orders.fulfilled", event.orderId(), event);
        log.info("Published OrderFulfilled for orderId={}", event.orderId());
    }
}
