package io.wcygan;

import com.jayway.jsonpath.JsonPath;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterAll;
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
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))
                    .withNetwork(network);

    private final PostgreSQLContainer<?> postgresContainer =
            new PostgreSQLContainer<>(
                    DockerImageName.parse("quay.io/debezium/postgres:18")
                            .asCompatibleSubstituteFor("postgres"))
                    .withNetwork(network)
                    .withNetworkAliases("postgres");

    private final DebeziumContainer debeziumContainer =
            new DebeziumContainer("quay.io/debezium/connect:3.3.1.Final")
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
        try (Connection connection = getConnection();
                Statement statement = connection.createStatement();
                KafkaConsumer<String, String> consumer = createConsumer()) {

            statement.execute("create schema todo");
            statement.execute(
                    "create table todo.Todo (id bigint not null, title varchar(255), primary key (id))");
            statement.execute("alter table todo.Todo replica identity full");
            statement.execute("insert into todo.Todo values (1, 'Learn CDC')");
            statement.execute("insert into todo.Todo values (2, 'Learn Debezium')");

            ConnectorConfiguration connectorConfiguration =
                    ConnectorConfiguration.forJdbcContainer(postgresContainer)
                            .with("topic.prefix", "dbserver1");

            debeziumContainer.registerConnector("todo-connector", connectorConfiguration);

            consumer.subscribe(List.of("dbserver1.todo.todo"));

            List<ConsumerRecord<String, String>> changeEvents = drain(consumer, 2, Duration.ofSeconds(20));

            ConsumerRecord<String, String> firstRecord = changeEvents.get(0);
            Number firstId = JsonPath.read(firstRecord.key(), "$.id");
            Assertions.assertEquals(1, firstId.intValue(), "first record key id");
            Assertions.assertEquals("r", JsonPath.read(firstRecord.value(), "$.op"), "first record operation");
            Assertions.assertEquals(
                    "Learn CDC", JsonPath.read(firstRecord.value(), "$.after.title"), "first record title");

            ConsumerRecord<String, String> secondRecord = changeEvents.get(1);
            Number secondId = JsonPath.read(secondRecord.key(), "$.id");
            Assertions.assertEquals(2, secondId.intValue(), "second record key id");
            Assertions.assertEquals("r", JsonPath.read(secondRecord.value(), "$.op"), "second record operation");
            Assertions.assertEquals(
                    "Learn Debezium", JsonPath.read(secondRecord.value(), "$.after.title"), "second record title");
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                postgresContainer.getJdbcUrl(), postgresContainer.getUsername(), postgresContainer.getPassword());
    }

    private KafkaConsumer<String, String> createConsumer() {
        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(),
                new StringDeserializer());
    }

    private List<ConsumerRecord<String, String>> drain(
            KafkaConsumer<String, String> consumer, int expectedRecordCount,
            Duration timeout) {

        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();

        while (System.nanoTime() < deadline && records.size() < expectedRecordCount) {
            consumer.poll(Duration.ofMillis(200)).forEach(records::add);
        }

        if (records.size() < expectedRecordCount) {
            throw new AssertionError(
                    "Expected " + expectedRecordCount + " change events but received " + records.size());
        }

        return records;
    }
}
