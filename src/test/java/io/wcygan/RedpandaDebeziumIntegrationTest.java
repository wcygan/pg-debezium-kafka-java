package io.wcygan;

import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

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

    private final RedpandaContainer redpandaContainer =
            new RedpandaContainer(DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.3.3"))
                    .withNetwork(network)
                    .withNetworkAliases("redpanda");

    private final PostgreSQLContainer<?> postgresContainer =
            new PostgreSQLContainer<>(
                    DockerImageName.parse("quay.io/debezium/postgres:18")
                            .asCompatibleSubstituteFor("postgres"))
                    .withNetwork(network)
                    .withNetworkAliases("postgres");

    private final GenericContainer<?> debeziumContainer =
            new GenericContainer<>(DockerImageName.parse("quay.io/debezium/connect:3.3.1.Final"))
                    .withNetwork(network)
                    .withEnv("BOOTSTRAP_SERVERS", "redpanda:9092")
                    .withEnv("GROUP_ID", "1")
                    .withEnv("CONFIG_STORAGE_TOPIC", "debezium_configs")
                    .withEnv("OFFSET_STORAGE_TOPIC", "debezium_offsets")
                    .withEnv("STATUS_STORAGE_TOPIC", "debezium_statuses")
                    .withEnv("KEY_CONVERTER_SCHEMAS_ENABLE", "false")
                    .withEnv("VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
                    .withExposedPorts(8083)
                    .waitingFor(Wait.forHttp("/connectors").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)))
                    .dependsOn(redpandaContainer);

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
        try (Connection connection = getConnection();
                Statement statement = connection.createStatement();
                KafkaConsumer<String, String> consumer = createConsumer()) {

            statement.execute("create schema todo");
            statement.execute(
                    "create table todo.Todo (id bigint not null, title varchar(255), primary key (id))");
            statement.execute("alter table todo.Todo replica identity full");
            statement.execute("insert into todo.Todo values (1, 'Learn CDC')");
            statement.execute("insert into todo.Todo values (2, 'Learn RedPanda')");

            Map<String, String> connectorConfig = new HashMap<>();
            connectorConfig.put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
            connectorConfig.put("tasks.max", "1");
            connectorConfig.put("database.hostname", "postgres");
            connectorConfig.put("database.port", "5432");
            connectorConfig.put("database.user", postgresContainer.getUsername());
            connectorConfig.put("database.password", postgresContainer.getPassword());
            connectorConfig.put("database.dbname", postgresContainer.getDatabaseName());
            connectorConfig.put("topic.prefix", "dbserver1");
            connectorConfig.put("schema.include.list", "todo");
            connectorConfig.put("plugin.name", "pgoutput");

            registerConnector("todo-connector", connectorConfig);

            // Wait for topic to be created
            Thread.sleep(5000);

            // List topics to verify topic exists
            var topics = consumer.listTopics();
            System.out.println("Available topics: " + topics.keySet());

            // Manually assign partition instead of subscribing to avoid consumer group coordination
            var partitionInfos = topics.get("dbserver1.todo.todo");
            System.out.println("Topic partitions: " + partitionInfos);

            if (partitionInfos != null && !partitionInfos.isEmpty()) {
                var topicPartitions = partitionInfos.stream()
                        .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                        .toList();
                consumer.assign(topicPartitions);
                consumer.seekToBeginning(topicPartitions);
                System.out.println("Assigned partitions: " + consumer.assignment());
            } else {
                throw new IllegalStateException("Topic dbserver1.todo.todo has no partitions");
            }

            System.out.println("About to start draining messages...");

            List<ConsumerRecord<String, String>> changeEvents = drain(consumer, 2, Duration.ofSeconds(20));

            System.out.println("Drained " + changeEvents.size() + " messages");

            ConsumerRecord<String, String> firstRecord = changeEvents.get(0);
            Number firstId = JsonPath.read(firstRecord.key(), "$.payload.id");
            Assertions.assertEquals(1, firstId.intValue(), "first record key id");
            Assertions.assertEquals("r", JsonPath.read(firstRecord.value(), "$.payload.op"), "first record operation");
            Assertions.assertEquals(
                    "Learn CDC", JsonPath.read(firstRecord.value(), "$.payload.after.title"), "first record title");

            ConsumerRecord<String, String> secondRecord = changeEvents.get(1);
            Number secondId = JsonPath.read(secondRecord.key(), "$.payload.id");
            Assertions.assertEquals(2, secondId.intValue(), "second record key id");
            Assertions.assertEquals("r", JsonPath.read(secondRecord.value(), "$.payload.op"), "second record operation");
            Assertions.assertEquals(
                    "Learn RedPanda", JsonPath.read(secondRecord.value(), "$.payload.after.title"), "second record title");
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                postgresContainer.getJdbcUrl(), postgresContainer.getUsername(), postgresContainer.getPassword());
    }

    private KafkaConsumer<String, String> createConsumer() {
        String bootstrapServers = redpandaContainer.getBootstrapServers();
        System.out.println("Consumer bootstrap servers: " + bootstrapServers);
        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(),
                new StringDeserializer());
    }

    private List<ConsumerRecord<String, String>> drain(
            KafkaConsumer<String, String> consumer, int expectedRecordCount,
            Duration timeout) {

        System.out.println("drain(): Starting to poll for " + expectedRecordCount + " records");
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();

        int pollCount = 0;
        while (System.nanoTime() < deadline && records.size() < expectedRecordCount) {
            var polled = consumer.poll(Duration.ofMillis(200));
            pollCount++;
            if (polled.count() > 0) {
                System.out.println("drain(): Poll #" + pollCount + " received " + polled.count() + " records");
                polled.forEach(r -> {
                    System.out.println("drain(): Record key: " + r.key().substring(0, Math.min(100, r.key().length())));
                    records.add(r);
                });
            }
        }

        System.out.println("drain(): Finished after " + pollCount + " polls, got " + records.size() + " records");

        if (records.size() < expectedRecordCount) {
            throw new AssertionError(
                    "Expected " + expectedRecordCount + " change events but received " + records.size());
        }

        return records;
    }

    private void registerConnector(String name, Map<String, String> config) throws IOException, InterruptedException {
        String connectorUrl = "http://" + debeziumContainer.getHost() + ":"
                + debeziumContainer.getMappedPort(8083) + "/connectors";

        // Build JSON payload
        StringBuilder json = new StringBuilder("{\"name\":\"" + name + "\",\"config\":{");
        config.forEach((key, value) -> json.append("\"").append(key).append("\":\"").append(value).append("\","));
        json.setLength(json.length() - 1); // Remove trailing comma
        json.append("}}");

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(connectorUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json.toString()))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 201) {
            throw new RuntimeException(
                    "Failed to register connector. Status: " + response.statusCode()
                            + ", Body: " + response.body());
        }
    }

    private String getConnectorStatus(String name) throws IOException, InterruptedException {
        String statusUrl = "http://" + debeziumContainer.getHost() + ":"
                + debeziumContainer.getMappedPort(8083) + "/connectors/" + name + "/status";

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(statusUrl))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }
}
