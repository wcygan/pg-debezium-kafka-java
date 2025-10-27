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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

class DebeziumIntegrationTest {

    private Network network;
    private KafkaContainer kafkaContainer;
    private PostgreSQLContainer<?> postgresContainer;
    private DebeziumContainer debeziumContainer;

    @BeforeEach
    void startContainers() {
        network = Network.newNetwork();
        kafkaContainer = new KafkaContainer(DockerImageName.parse(TestConstants.KAFKA_IMAGE))
                .withNetwork(network);
        postgresContainer = createPostgresContainer(network);
        debeziumContainer = new DebeziumContainer(TestConstants.DEBEZIUM_IMAGE)
                .withNetwork(network)
                .withKafka(kafkaContainer)
                .dependsOn(kafkaContainer);

        Startables.deepStart(Stream.of(kafkaContainer, postgresContainer, debeziumContainer))
                .join();
    }

    @AfterEach
    void stopContainers() {
        if (debeziumContainer != null) debeziumContainer.stop();
        if (kafkaContainer != null) kafkaContainer.stop();
        if (postgresContainer != null) postgresContainer.stop();
        if (network != null) network.close();
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

    @Test
    void postgresConnectorCapturesUpdateOperations() throws Exception {
        try (Connection connection = getConnection(postgresContainer);
                KafkaConsumer<String, String> consumer = createConsumer(kafkaContainer.getBootstrapServers())) {

            // Setup database
            setupTestDatabase(connection);
            insertTestRecord(connection, 1, "Original Title");

            // Register Debezium connector
            ConnectorConfiguration connectorConfiguration =
                    ConnectorConfiguration.forJdbcContainer(postgresContainer)
                            .with("topic.prefix", TestConstants.TOPIC_PREFIX);
            debeziumContainer.registerConnector("update-connector", connectorConfiguration);

            consumer.subscribe(List.of(getTestTopicName()));

            // Drain initial snapshot event
            drainEvents(consumer, 1, TestConstants.EVENT_CONSUMPTION_TIMEOUT);

            // Perform update
            updateTestRecord(connection, 1, "Updated Title");

            // Consume update event
            List<ConsumerRecord<String, String>> records =
                    drainEvents(consumer, 1, TestConstants.EVENT_CONSUMPTION_TIMEOUT);
            List<CdcEvent> events = toCdcEvents(records);

            // Validate update event
            CdcEvent updateEvent = events.get(0);
            Assertions.assertTrue(updateEvent.isUpdateOperation());
            Assertions.assertEquals(1, updateEvent.getId());
            Assertions.assertEquals("Updated Title", updateEvent.getTitle());
            Assertions.assertTrue(updateEvent.hasBeforeState());
            Assertions.assertEquals("Original Title", updateEvent.getBeforeTitle());
        }
    }

    @Test
    void postgresConnectorCapturesDeleteOperations() throws Exception {
        try (Connection connection = getConnection(postgresContainer);
                KafkaConsumer<String, String> consumer = createConsumer(kafkaContainer.getBootstrapServers())) {

            // Setup database
            setupTestDatabase(connection);
            insertTestRecord(connection, 1, "To Be Deleted");

            // Register Debezium connector
            ConnectorConfiguration connectorConfiguration =
                    ConnectorConfiguration.forJdbcContainer(postgresContainer)
                            .with("topic.prefix", TestConstants.TOPIC_PREFIX);
            debeziumContainer.registerConnector("delete-connector", connectorConfiguration);

            consumer.subscribe(List.of(getTestTopicName()));

            // Drain initial snapshot event
            drainEvents(consumer, 1, TestConstants.EVENT_CONSUMPTION_TIMEOUT);

            // Perform delete
            deleteTestRecord(connection, 1);

            // Consume delete event
            List<ConsumerRecord<String, String>> records =
                    drainEvents(consumer, 1, TestConstants.EVENT_CONSUMPTION_TIMEOUT);
            List<CdcEvent> events = toCdcEvents(records);

            // Validate delete event
            CdcEvent deleteEvent = events.get(0);
            Assertions.assertTrue(deleteEvent.isDeleteOperation());
            Assertions.assertEquals(1, deleteEvent.getId());
            Assertions.assertTrue(deleteEvent.hasBeforeState());
            Assertions.assertEquals("To Be Deleted", deleteEvent.getBeforeTitle());
            Assertions.assertFalse(deleteEvent.hasAfterState());
        }
    }

    @Test
    void postgresConnectorCapturesMultipleUpdates() throws Exception {
        try (Connection connection = getConnection(postgresContainer);
                KafkaConsumer<String, String> consumer = createConsumer(kafkaContainer.getBootstrapServers())) {

            // Setup database
            setupTestDatabase(connection);
            insertTestRecord(connection, 1, "Version 1");

            // Register Debezium connector
            ConnectorConfiguration connectorConfiguration =
                    ConnectorConfiguration.forJdbcContainer(postgresContainer)
                            .with("topic.prefix", TestConstants.TOPIC_PREFIX);
            debeziumContainer.registerConnector("multi-update-connector", connectorConfiguration);

            consumer.subscribe(List.of(getTestTopicName()));

            // Drain initial snapshot event
            drainEvents(consumer, 1, TestConstants.EVENT_CONSUMPTION_TIMEOUT);

            // Perform multiple updates
            updateTestRecord(connection, 1, "Version 2");
            updateTestRecord(connection, 1, "Version 3");
            updateTestRecord(connection, 1, "Version 4");

            // Consume all update events
            List<ConsumerRecord<String, String>> records =
                    drainEvents(consumer, 3, TestConstants.EVENT_CONSUMPTION_TIMEOUT);
            List<CdcEvent> events = toCdcEvents(records);

            // Validate all three updates
            Assertions.assertEquals(3, events.size());
            Assertions.assertTrue(events.stream().allMatch(CdcEvent::isUpdateOperation));

            Assertions.assertEquals("Version 2", events.get(0).getTitle());
            Assertions.assertEquals("Version 1", events.get(0).getBeforeTitle());

            Assertions.assertEquals("Version 3", events.get(1).getTitle());
            Assertions.assertEquals("Version 2", events.get(1).getBeforeTitle());

            Assertions.assertEquals("Version 4", events.get(2).getTitle());
            Assertions.assertEquals("Version 3", events.get(2).getBeforeTitle());
        }
    }

    @Test
    void postgresConnectorCapturesBulkOperations() throws Exception {
        try (Connection connection = getConnection(postgresContainer);
                KafkaConsumer<String, String> consumer = createConsumer(kafkaContainer.getBootstrapServers())) {

            // Setup database
            setupTestDatabase(connection);

            Object[][] bulkRecords = {
                {1L, "Task 1"}, {2L, "Task 2"}, {3L, "Task 3"}, {4L, "Task 4"}, {5L, "Task 5"}
            };
            bulkInsertTestRecords(connection, bulkRecords);

            // Register Debezium connector
            ConnectorConfiguration connectorConfiguration =
                    ConnectorConfiguration.forJdbcContainer(postgresContainer)
                            .with("topic.prefix", TestConstants.TOPIC_PREFIX);
            debeziumContainer.registerConnector("bulk-connector", connectorConfiguration);

            consumer.subscribe(List.of(getTestTopicName()));

            // Consume all snapshot events
            List<ConsumerRecord<String, String>> records =
                    drainEvents(consumer, 5, TestConstants.EVENT_CONSUMPTION_TIMEOUT);
            List<CdcEvent> events = toCdcEvents(records);

            // Validate all records captured
            Assertions.assertEquals(5, events.size());
            Assertions.assertTrue(events.stream().allMatch(CdcEvent::isReadOperation));

            for (int i = 0; i < 5; i++) {
                Assertions.assertEquals(i + 1, events.get(i).getId());
                Assertions.assertEquals("Task " + (i + 1), events.get(i).getTitle());
            }
        }
    }

    @Test
    void postgresConnectorCapturesTransactionalChanges() throws Exception {
        try (Connection connection = getConnection(postgresContainer);
                KafkaConsumer<String, String> consumer = createConsumer(kafkaContainer.getBootstrapServers())) {

            // Setup database with initial data to ensure connector starts properly
            setupTestDatabase(connection);
            insertTestRecord(connection, 999, "Warmup Record");

            // Register Debezium connector
            ConnectorConfiguration connectorConfiguration =
                    ConnectorConfiguration.forJdbcContainer(postgresContainer)
                            .with("topic.prefix", TestConstants.TOPIC_PREFIX);
            debeziumContainer.registerConnector("tx-connector", connectorConfiguration);

            consumer.subscribe(List.of(getTestTopicName()));

            // Drain warmup snapshot event to ensure connector is ready
            drainEvents(consumer, 1, TestConstants.EVENT_CONSUMPTION_TIMEOUT);

            // Perform transactional insert, update, delete
            connection.setAutoCommit(false);
            insertTestRecord(connection, 1, "Transactional Record");
            updateTestRecord(connection, 1, "Updated in Transaction");
            deleteTestRecord(connection, 1);
            connection.commit();
            connection.setAutoCommit(true);

            // Consume all events - need to get enough to capture create, update, delete
            // (may also include delete tombstone)
            List<ConsumerRecord<String, String>> records =
                    drainEvents(consumer, 4, TestConstants.EVENT_CONSUMPTION_TIMEOUT);
            List<CdcEvent> allEvents = toCdcEvents(records);

            // Filter to only events for ID 1 (the transactional record)
            // Note: delete operation creates 2 events - the delete event + tombstone (null value)
            List<CdcEvent> events = allEvents.stream()
                    .filter(e -> e.getId() == 1)
                    .toList();

            // Validate event sequence (should be at least 3, may include tombstone)
            Assertions.assertTrue(events.size() >= 3, "Expected at least 3 events, got " + events.size());
            Assertions.assertTrue(events.get(0).isCreateOperation());
            Assertions.assertTrue(events.get(1).isUpdateOperation());
            Assertions.assertTrue(events.get(2).isDeleteOperation());

            Assertions.assertEquals("Transactional Record", events.get(0).getTitle());
            Assertions.assertEquals("Updated in Transaction", events.get(1).getTitle());
            Assertions.assertEquals("Updated in Transaction", events.get(2).getBeforeTitle());
        }
    }

}
