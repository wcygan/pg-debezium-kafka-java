package io.wcygan;

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
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Isolated test for RedPanda connectivity and basic produce/consume functionality.
 */
@TestInstance(Lifecycle.PER_CLASS)
class RedpandaConnectivityTest {

    private final GenericContainer<?> redpandaContainer =
            new GenericContainer<>(DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.3.3"))
                    .withCommand(
                            "redpanda",
                            "start",
                            "--mode=dev-container",
                            "--kafka-addr=0.0.0.0:9092")
                    .withExposedPorts(9092)
                    .waitingFor(Wait.forLogMessage(".*Successfully started Redpanda!.*", 1)
                            .withStartupTimeout(Duration.ofMinutes(2)));

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
        String bootstrapServers = redpandaContainer.getHost() + ":" + redpandaContainer.getMappedPort(9092);
        String topicName = "test-topic-" + UUID.randomUUID();

        System.out.println("Bootstrap servers: " + bootstrapServers);

        // Create topic
        try (AdminClient adminClient = AdminClient.create(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            adminClient.createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all().get();
            System.out.println("Topic created: " + topicName);
        }

        // Produce messages
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ProducerConfig.CLIENT_ID_CONFIG, "test-producer"),
                new StringSerializer(),
                new StringSerializer())) {

            producer.send(new ProducerRecord<>(topicName, "key1", "value1")).get();
            producer.send(new ProducerRecord<>(topicName, "key2", "value2")).get();
            System.out.println("Produced 2 messages");
        }

        // Consume messages
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

            // First poll with longer timeout for partition assignment
            consumer.poll(Duration.ofMillis(1000)).forEach(r -> {
                System.out.println("Received: key=" + r.key() + ", value=" + r.value());
                records.add(r);
            });

            while (System.nanoTime() < deadline && records.size() < 2) {
                consumer.poll(Duration.ofMillis(200)).forEach(r -> {
                    System.out.println("Received: key=" + r.key() + ", value=" + r.value());
                    records.add(r);
                });
            }

            Assertions.assertEquals(2, records.size(), "Should receive 2 messages");
            Assertions.assertEquals("value1", records.get(0).value());
            Assertions.assertEquals("value2", records.get(1).value());
        }
    }
}
