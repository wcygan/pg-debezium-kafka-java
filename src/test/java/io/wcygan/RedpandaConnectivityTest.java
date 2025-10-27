package io.wcygan;

import static io.wcygan.testutil.ContainerHelper.createSimpleRedpandaContainer;

import io.wcygan.testutil.TestConstants;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.containers.GenericContainer;

/**
 * Isolated test for RedPanda connectivity and basic produce/consume functionality.
 */
@TestInstance(Lifecycle.PER_CLASS)
class RedpandaConnectivityTest {

    private final GenericContainer<?> redpandaContainer = createSimpleRedpandaContainer();

    @BeforeAll
    void startContainer() {
        redpandaContainer.start();
    }

    @AfterAll
    void stopContainer() {
        redpandaContainer.stop();
    }

    @Test
    void canProduceAndConsumeMessages() throws Exception {
        String bootstrapServers =
                redpandaContainer.getHost() + ":" + redpandaContainer.getMappedPort(TestConstants.REDPANDA_KAFKA_PORT);
        String topicName = "test-topic-" + UUID.randomUUID();

        createTopic(bootstrapServers, topicName);
        produceMessages(bootstrapServers, topicName);
        consumeAndValidateMessages(bootstrapServers, topicName);
    }

    private void createTopic(String bootstrapServers, String topicName) throws Exception {
        try (AdminClient adminClient = AdminClient.create(
                Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            adminClient.createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all().get();
        }
    }

    private void produceMessages(String bootstrapServers, String topicName) throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ProducerConfig.CLIENT_ID_CONFIG, "test-producer"),
                new StringSerializer(),
                new StringSerializer())) {

            producer.send(new ProducerRecord<>(topicName, "key1", "value1")).get();
            producer.send(new ProducerRecord<>(topicName, "key2", "value2")).get();
        }
    }

    private void consumeAndValidateMessages(String bootstrapServers, String topicName) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(),
                new StringDeserializer())) {

            consumer.subscribe(List.of(topicName));

            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();

            consumer.poll(TestConstants.POLL_TIMEOUT_FIRST).forEach(records::add);

            while (System.nanoTime() < deadline && records.size() < 2) {
                consumer.poll(TestConstants.POLL_TIMEOUT).forEach(records::add);
            }

            Assertions.assertEquals(2, records.size(), "Should receive 2 messages");
            Assertions.assertEquals("value1", records.get(0).value());
            Assertions.assertEquals("value2", records.get(1).value());
        }
    }
}
