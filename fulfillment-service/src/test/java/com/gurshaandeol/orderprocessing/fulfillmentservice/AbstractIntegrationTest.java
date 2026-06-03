package com.gurshaandeol.orderprocessing.fulfillmentservice;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
public abstract class AbstractIntegrationTest {

    @Container
    static final MongoDBContainer mongoDBContainer =
            new MongoDBContainer("mongo:7.0")
                    ;

    @Container
    static final KafkaContainer kafkaContainer =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))
                    ;

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongoDBContainer::getReplicaSetUrl);
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
        registry.add("spring.kafka.producer.bootstrap-servers", kafkaContainer::getBootstrapServers);
        registry.add("spring.kafka.consumer.bootstrap-servers", kafkaContainer::getBootstrapServers);
    }

    /** Polls the given Kafka topic for one record using a unique consumer group. */
    protected ConsumerRecord<String, String> pollTopic(String topic) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                kafkaContainer.getBootstrapServers(),
                "test-group-" + UUID.randomUUID(),
                "true"
        );
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (Consumer<String, String> consumer =
                     new DefaultKafkaConsumerFactory<>(consumerProps,
                             new StringDeserializer(), new StringDeserializer())
                             .createConsumer()) {
            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<String, String> records =
                    KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
            return records.iterator().hasNext()
                    ? records.iterator().next()
                    : null;
        }
    }

    /** Polls the given topic until a record with the specified key is found, within 10 seconds. */
    protected ConsumerRecord<String, String> pollTopicForKey(String topic, String expectedKey) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                kafkaContainer.getBootstrapServers(),
                "test-group-" + UUID.randomUUID(),
                "true"
        );
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (Consumer<String, String> consumer =
                     new DefaultKafkaConsumerFactory<>(consumerProps,
                             new StringDeserializer(), new StringDeserializer())
                             .createConsumer()) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (expectedKey.equals(record.key())) {
                        return record;
                    }
                }
            }
            return null;
        }
    }
}
