package io.wcygan.testutil;

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
import java.util.HashMap;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Helper utilities for CDC testing operations.
 */
public final class CdcTestHelper {

    private CdcTestHelper() {
        // Utility class
    }

    /**
     * Creates a database connection to the PostgreSQL container.
     *
     * @param postgresContainer the PostgreSQL container
     * @return JDBC connection
     * @throws SQLException if connection fails
     */
    public static Connection getConnection(PostgreSQLContainer<?> postgresContainer) throws SQLException {
        return DriverManager.getConnection(
                postgresContainer.getJdbcUrl(),
                postgresContainer.getUsername(),
                postgresContainer.getPassword());
    }

    /**
     * Sets up the test database schema and inserts sample data.
     *
     * @param connection database connection
     * @throws SQLException if database operations fail
     */
    public static void setupTestDatabase(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("create schema if not exists " + TestConstants.SCHEMA_NAME);
            statement.execute("drop table if exists " + TestConstants.FULL_TABLE_NAME);
            statement.execute(
                    "create table " + TestConstants.FULL_TABLE_NAME
                            + " (id bigint not null, title varchar(255), primary key (id))");
            statement.execute("alter table " + TestConstants.FULL_TABLE_NAME + " replica identity full");
        }
    }

    /**
     * Inserts test data into the database.
     *
     * @param connection database connection
     * @param id record ID
     * @param title record title
     * @throws SQLException if insert fails
     */
    public static void insertTestRecord(Connection connection, long id, String title) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    "insert into " + TestConstants.FULL_TABLE_NAME
                            + " values (" + id + ", '" + title + "')");
        }
    }

    /**
     * Updates a test record in the database.
     *
     * @param connection database connection
     * @param id record ID to update
     * @param newTitle new title value
     * @throws SQLException if update fails
     */
    public static void updateTestRecord(Connection connection, long id, String newTitle) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    "update " + TestConstants.FULL_TABLE_NAME
                            + " set title = '" + newTitle + "' where id = " + id);
        }
    }

    /**
     * Deletes a test record from the database.
     *
     * @param connection database connection
     * @param id record ID to delete
     * @throws SQLException if delete fails
     */
    public static void deleteTestRecord(Connection connection, long id) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("delete from " + TestConstants.FULL_TABLE_NAME + " where id = " + id);
        }
    }

    /**
     * Inserts multiple test records in a single transaction.
     *
     * @param connection database connection
     * @param records array of (id, title) pairs
     * @throws SQLException if bulk insert fails
     */
    public static void bulkInsertTestRecords(Connection connection, Object[][] records) throws SQLException {
        connection.setAutoCommit(false);
        try (Statement statement = connection.createStatement()) {
            for (Object[] record : records) {
                long id = (Long) record[0];
                String title = (String) record[1];
                statement.execute(
                        "insert into " + TestConstants.FULL_TABLE_NAME
                                + " values (" + id + ", '" + title + "')");
            }
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    /**
     * Creates a Debezium connector configuration for PostgreSQL.
     *
     * @param postgresContainer PostgreSQL container to connect to
     * @return connector configuration map
     */
    public static Map<String, String> createConnectorConfig(PostgreSQLContainer<?> postgresContainer) {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", TestConstants.CONNECTOR_CLASS);
        config.put("tasks.max", TestConstants.MAX_TASKS);
        config.put("database.hostname", TestConstants.POSTGRES_ALIAS);
        config.put("database.port", "5432");
        config.put("database.user", postgresContainer.getUsername());
        config.put("database.password", postgresContainer.getPassword());
        config.put("database.dbname", postgresContainer.getDatabaseName());
        config.put("topic.prefix", TestConstants.TOPIC_PREFIX);
        config.put("schema.include.list", TestConstants.SCHEMA_NAME);
        config.put("plugin.name", TestConstants.PLUGIN_NAME);
        return config;
    }

    /**
     * Registers a Debezium connector via HTTP API.
     *
     * @param debeziumContainer Debezium Connect container
     * @param connectorName name of the connector
     * @param config connector configuration
     * @throws IOException if HTTP request fails
     * @throws InterruptedException if request is interrupted
     */
    public static void registerConnector(
            GenericContainer<?> debeziumContainer,
            String connectorName,
            Map<String, String> config)
            throws IOException, InterruptedException {

        String connectorUrl = "http://" + debeziumContainer.getHost() + ":"
                + debeziumContainer.getMappedPort(TestConstants.DEBEZIUM_PORT) + "/connectors";

        StringBuilder json = new StringBuilder("{\"name\":\"" + connectorName + "\",\"config\":{");
        config.forEach((key, value) -> json.append("\"").append(key).append("\":\"").append(value).append("\","));
        json.setLength(json.length() - 1);
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

    /**
     * Waits for the Debezium snapshot to complete by polling container logs.
     *
     * @param debeziumContainer Debezium Connect container
     * @param timeout maximum time to wait
     * @throws InterruptedException if wait is interrupted
     * @throws AssertionError if snapshot doesn't complete within timeout
     */
    public static void waitForSnapshotCompletion(GenericContainer<?> debeziumContainer, Duration timeout)
            throws InterruptedException {

        long deadline = System.nanoTime() + timeout.toNanos();

        while (System.nanoTime() < deadline) {
            String logs = debeziumContainer.getLogs();
            if (logs.contains("Snapshot") && logs.contains("completed")) {
                return;
            }
            Thread.sleep(500);
        }

        throw new AssertionError("Debezium snapshot did not complete within " + timeout.toSeconds() + " seconds");
    }

    /**
     * Generates the full Kafka topic name for a table.
     *
     * @param schemaName database schema name
     * @param tableName database table name
     * @return fully qualified Kafka topic name
     */
    public static String getTopicName(String schemaName, String tableName) {
        return TestConstants.TOPIC_PREFIX + "." + schemaName.toLowerCase() + "." + tableName.toLowerCase();
    }

    /**
     * Gets the topic name for the default test table.
     *
     * @return Kafka topic name
     */
    public static String getTestTopicName() {
        return getTopicName(TestConstants.SCHEMA_NAME, TestConstants.TABLE_NAME);
    }
}
