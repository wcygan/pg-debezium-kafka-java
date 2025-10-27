package io.wcygan;

import static io.wcygan.testutil.CdcTestHelper.*;
import static io.wcygan.testutil.ContainerHelper.*;
import static io.wcygan.testutil.KafkaTestHelper.*;

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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.redpanda.RedpandaContainer;

/**
 * PostgreSQL CDC demonstration using Debezium and RedPanda.
 *
 * <p>This test uses a custom GenericContainer configuration for Debezium Connect instead of
 * DebeziumContainer to work around type incompatibility issues with RedPandaContainer.
 *
 * <p>Key differences from Kafka version:
 * <ul>
 *   <li>Uses RedPandaContainer instead of KafkaContainer</li>
 *   <li>Uses GenericContainer for Debezium with manual environment configuration</li>
 *   <li>Registers connectors via HTTP API instead of DebeziumContainer helper methods</li>
 * </ul>
 */
@TestInstance(Lifecycle.PER_CLASS)
class RedpandaDebeziumIntegrationTest {

    private final Network network = Network.newNetwork();
    private final RedpandaContainer redpandaContainer = createRedpandaContainer(network);
    private final PostgreSQLContainer<?> postgresContainer = createPostgresContainer(network);
    private final GenericContainer<?> debeziumContainer = createDebeziumContainer(network, redpandaContainer);

    @BeforeAll
    void startContainers() {
        Startables.deepStart(Stream.of(redpandaContainer, postgresContainer, debeziumContainer))
                .join();
    }

    @AfterAll
    void stopContainers() {
        debeziumContainer.stop();
        redpandaContainer.stop();
        postgresContainer.stop();
        network.close();
    }

    @Test
    void postgresConnectorCapturesChanges() throws Exception {
        try (Connection connection = getConnection(postgresContainer);
                KafkaConsumer<String, String> consumer = createConsumer(redpandaContainer.getBootstrapServers())) {

            // Setup database
            setupTestDatabase(connection);
            insertTestRecord(connection, 1, "Learn CDC");
            insertTestRecord(connection, 2, "Learn RedPanda");

            // Register Debezium connector
            registerConnector(debeziumContainer, "todo-connector", createConnectorConfig(postgresContainer));

            // Wait for snapshot to complete
            waitForSnapshotCompletion(debeziumContainer, TestConstants.SNAPSHOT_TIMEOUT);

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
            Assertions.assertEquals("Learn RedPanda", secondEvent.getTitle());
        }
    }

}
