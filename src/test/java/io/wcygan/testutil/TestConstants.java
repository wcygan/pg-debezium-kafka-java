package io.wcygan.testutil;

import java.time.Duration;

/**
 * Test configuration constants for CDC integration tests.
 */
public final class TestConstants {

    private TestConstants() {
        // Utility class
    }

    // Container Images
    public static final String REDPANDA_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v23.3.3";
    public static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.6.0";
    public static final String POSTGRES_IMAGE = "quay.io/debezium/postgres:18";
    public static final String DEBEZIUM_IMAGE = "quay.io/debezium/connect:3.3.1.Final";

    // Network Configuration
    public static final String REDPANDA_ALIAS = "redpanda";
    public static final String POSTGRES_ALIAS = "postgres";
    public static final int REDPANDA_KAFKA_PORT = 9092;
    public static final int REDPANDA_INTERNAL_PORT = 19092;
    public static final int DEBEZIUM_PORT = 8083;

    // Kafka Topics
    public static final String TOPIC_PREFIX = "dbserver1";

    // Database Schema
    public static final String SCHEMA_NAME = "todo";
    public static final String TABLE_NAME = "Todo";
    public static final String FULL_TABLE_NAME = SCHEMA_NAME + "." + TABLE_NAME;

    // Timeouts
    public static final Duration CONTAINER_STARTUP_TIMEOUT = Duration.ofMinutes(3);
    public static final Duration REDPANDA_STARTUP_TIMEOUT = Duration.ofMinutes(2);
    public static final Duration SNAPSHOT_TIMEOUT = Duration.ofSeconds(30);
    public static final Duration EVENT_CONSUMPTION_TIMEOUT = Duration.ofSeconds(30);
    public static final Duration POLL_TIMEOUT_FIRST = Duration.ofMillis(1000);
    public static final Duration POLL_TIMEOUT = Duration.ofMillis(200);

    // Debezium Configuration
    public static final String CONNECTOR_CLASS = "io.debezium.connector.postgresql.PostgresConnector";
    public static final String PLUGIN_NAME = "pgoutput";
    public static final String MAX_TASKS = "1";
    public static final String GROUP_ID = "1";

    // Kafka Connect Topics
    public static final String CONFIG_STORAGE_TOPIC = "debezium_configs";
    public static final String OFFSET_STORAGE_TOPIC = "debezium_offsets";
    public static final String STATUS_STORAGE_TOPIC = "debezium_statuses";
}
