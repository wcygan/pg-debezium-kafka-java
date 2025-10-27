package io.wcygan.testutil;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Helper utilities for creating and configuring TestContainers.
 */
public final class ContainerHelper {

    private ContainerHelper() {
        // Utility class
    }

    /**
     * Creates a RedPanda container configured for CDC testing with dual listeners.
     *
     * @param network Docker network to attach to
     * @return configured RedpandaContainer
     */
    public static RedpandaContainer createRedpandaContainer(Network network) {
        return new RedpandaContainer(DockerImageName.parse(TestConstants.REDPANDA_IMAGE))
                .withListener(() -> TestConstants.REDPANDA_ALIAS + ":" + TestConstants.REDPANDA_INTERNAL_PORT)
                .withNetwork(network)
                .withNetworkAliases(TestConstants.REDPANDA_ALIAS);
    }

    /**
     * Creates a simple RedPanda container for basic connectivity testing.
     *
     * @return configured GenericContainer running RedPanda
     */
    public static GenericContainer<?> createSimpleRedpandaContainer() {
        return new GenericContainer<>(DockerImageName.parse(TestConstants.REDPANDA_IMAGE))
                .withCommand(
                        "redpanda",
                        "start",
                        "--mode=dev-container",
                        "--kafka-addr=0.0.0.0:" + TestConstants.REDPANDA_KAFKA_PORT)
                .withExposedPorts(TestConstants.REDPANDA_KAFKA_PORT)
                .waitingFor(Wait.forLogMessage(".*Successfully started Redpanda!.*", 1)
                        .withStartupTimeout(TestConstants.REDPANDA_STARTUP_TIMEOUT));
    }

    /**
     * Creates a PostgreSQL container configured for CDC testing.
     *
     * @param network Docker network to attach to
     * @return configured PostgreSQLContainer
     */
    public static PostgreSQLContainer<?> createPostgresContainer(Network network) {
        return new PostgreSQLContainer<>(
                DockerImageName.parse(TestConstants.POSTGRES_IMAGE)
                        .asCompatibleSubstituteFor("postgres"))
                .withNetwork(network)
                .withNetworkAliases(TestConstants.POSTGRES_ALIAS);
    }

    /**
     * Creates a Debezium Connect container configured for RedPanda.
     *
     * @param network Docker network to attach to
     * @param redpandaContainer RedPanda container to connect to
     * @return configured GenericContainer running Debezium Connect
     */
    public static GenericContainer<?> createDebeziumContainer(
            Network network,
            RedpandaContainer redpandaContainer) {

        return new GenericContainer<>(DockerImageName.parse(TestConstants.DEBEZIUM_IMAGE))
                .withNetwork(network)
                .withEnv("BOOTSTRAP_SERVERS",
                        TestConstants.REDPANDA_ALIAS + ":" + TestConstants.REDPANDA_INTERNAL_PORT)
                .withEnv("GROUP_ID", TestConstants.GROUP_ID)
                .withEnv("CONFIG_STORAGE_TOPIC", TestConstants.CONFIG_STORAGE_TOPIC)
                .withEnv("OFFSET_STORAGE_TOPIC", TestConstants.OFFSET_STORAGE_TOPIC)
                .withEnv("STATUS_STORAGE_TOPIC", TestConstants.STATUS_STORAGE_TOPIC)
                .withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false")
                .withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
                .withExposedPorts(TestConstants.DEBEZIUM_PORT)
                .waitingFor(Wait.forHttp("/connectors")
                        .forStatusCode(200)
                        .withStartupTimeout(TestConstants.CONTAINER_STARTUP_TIMEOUT))
                .dependsOn(redpandaContainer);
    }
}
