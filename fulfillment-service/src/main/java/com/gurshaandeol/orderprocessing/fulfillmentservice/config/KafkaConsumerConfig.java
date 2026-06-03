package com.gurshaandeol.orderprocessing.fulfillmentservice.config;

import com.gurshaandeol.orderprocessing.common.event.PaymentProcessed;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentProcessed> kafkaListenerContainerFactory(
            ConsumerFactory<String, PaymentProcessed> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, PaymentProcessed> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}
