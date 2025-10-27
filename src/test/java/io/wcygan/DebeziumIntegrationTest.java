package io.wcygan;

import static io.wcygan.testutil.CdcTestHelper.*;
import static io.wcygan.testutil.ContainerHelper.createPostgresContainer;
import static io.wcygan.testutil.KafkaTestHelper.*;

import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import io.wcygan.testutil.CdcEvent;
import io.wcygan.testutil.TestConstants;
import java.sql.Connection;
import java.util.List;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

@TestInstance(Lifecycle.PER_CLASS)
class DebeziumIntegrationTest {

    private final Network network = Network.newNetwork();

    private final KafkaContainer kafkaContainer =
            new KafkaContainer(DockerImageName.parse(TestConstants.KAFKA_IMAGE))
                    .withNetwork(network);

    private final PostgreSQLContainer<?> postgresContainer = createPostgresContainer(network);

    private final DebeziumContainer debeziumContainer =
            new DebeziumContainer(TestConstants.DEBEZIUM_IMAGE)
                    .withNetwork(network)
                    .withKafka(kafkaContainer)
                    .dependsOn(kafkaContainer);

    @BeforeAll
    void startContainers() {
        Startables.deepStart(Stream.of(kafkaContainer, postgresContainer, debeziumContainer))
                .join();
    }

    @AfterAll
    void stopContainers() {
        debeziumContainer.stop();
        kafkaContainer.stop();
        postgresContainer.stop();
        network.close();
    }

    @Test
    void postgresConnectorCapturesChanges() throws Exception {
        try (Connection connection = getConnection(postgresContainer);
                KafkaConsumer<String, String> consumer = createConsumer(kafkaContainer.getBootstrapServers())) {

            // Setup database
            setupTestDatabase(connection);
            insertTestRecord(connection, 1, "Learn CDC");
            insertTestRecord(connection, 2, "Learn Debezium");

            // Register Debezium connector using DebeziumContainer helper
            ConnectorConfiguration connectorConfiguration =
                    ConnectorConfiguration.forJdbcContainer(postgresContainer)
                            .with("topic.prefix", TestConstants.TOPIC_PREFIX);

            debeziumContainer.registerConnector("todo-connector", connectorConfiguration);

            // Consume CDC events
            consumer.subscribe(List.of(getTestTopicName()));
            List<ConsumerRecord<String, String>> records =
                    drainEvents(consumer, 2, TestConstants.EVENT_CONSUMPTION_TIMEOUT);

            // Validate events
            List<CdcEvent> events = toCdcEvents(records);

            CdcEvent firstEvent = events.get(0);
            Assertions.assertEquals(1, firstEvent.getId());
            Assertions.assertTrue(firstEvent.isReadOperation());
            Assertions.assertEquals("Learn CDC", firstEvent.getTitle());

            CdcEvent secondEvent = events.get(1);
            Assertions.assertEquals(2, secondEvent.getId());
            Assertions.assertTrue(secondEvent.isReadOperation());
            Assertions.assertEquals("Learn Debezium", secondEvent.getTitle());
        }
    }

}
