/**
 * Test utilities and helper classes for CDC integration testing.
 *
 * <h2>Core Components</h2>
 *
 * <h3>{@link io.wcygan.testutil.TestConstants}</h3>
 * Centralized test configuration including:
 * <ul>
 *   <li>Container image versions (RedPanda, Kafka, PostgreSQL, Debezium)</li>
 *   <li>Network configuration (aliases, ports)</li>
 *   <li>Timeouts for various operations</li>
 *   <li>Database schema and table names</li>
 *   <li>Debezium connector settings</li>
 * </ul>
 *
 * <h3>{@link io.wcygan.testutil.CdcEvent}</h3>
 * Type-safe value object for CDC events providing:
 * <ul>
 *   <li>Convenient accessors for event fields (id, operation, title)</li>
 *   <li>Operation type checking (isReadOperation, isCreateOperation, etc.)</li>
 *   <li>Eliminates JsonPath boilerplate in assertions</li>
 * </ul>
 *
 * Example usage:
 * <pre>{@code
 * CdcEvent event = CdcEvent.from(consumerRecord);
 * Assertions.assertEquals(1, event.getId());
 * Assertions.assertTrue(event.isReadOperation());
 * Assertions.assertEquals("Learn CDC", event.getTitle());
 * }</pre>
 *
 * <h3>{@link io.wcygan.testutil.KafkaTestHelper}</h3>
 * Kafka consumer operations:
 * <ul>
 *   <li>createConsumer() - Factory method for test consumers</li>
 *   <li>drainEvents() - Poll consumer until expected event count reached</li>
 *   <li>toCdcEvents() - Convert ConsumerRecords to CdcEvent objects</li>
 * </ul>
 *
 * <h3>{@link io.wcygan.testutil.ContainerHelper}</h3>
 * TestContainers factory methods:
 * <ul>
 *   <li>createRedpandaContainer() - RedPanda with dual listeners for CDC</li>
 *   <li>createSimpleRedpandaContainer() - Basic RedPanda for connectivity tests</li>
 *   <li>createPostgresContainer() - PostgreSQL with CDC enabled</li>
 *   <li>createDebeziumContainer() - Debezium Connect configured for RedPanda</li>
 * </ul>
 *
 * <h3>{@link io.wcygan.testutil.CdcTestHelper}</h3>
 * CDC-specific operations:
 * <ul>
 *   <li>getConnection() - Create PostgreSQL connection</li>
 *   <li>setupTestDatabase() - Create schema and table</li>
 *   <li>insertTestRecord() - Insert test data</li>
 *   <li>createConnectorConfig() - Build Debezium connector configuration</li>
 *   <li>registerConnector() - Register connector via HTTP API</li>
 *   <li>waitForSnapshotCompletion() - Poll logs for snapshot completion</li>
 *   <li>getTestTopicName() - Generate Kafka topic name</li>
 * </ul>
 *
 * <h2>Design Principles</h2>
 * <ul>
 *   <li><b>Single Responsibility</b> - Each helper class handles one aspect of testing</li>
 *   <li><b>Reusability</b> - Methods designed for use across multiple test classes</li>
 *   <li><b>Type Safety</b> - CdcEvent prevents JsonPath typos and runtime errors</li>
 *   <li><b>Readability</b> - Tests read like specifications using fluent APIs</li>
 *   <li><b>Maintainability</b> - Configuration changes in one place (TestConstants)</li>
 * </ul>
 *
 * <h2>Example Integration</h2>
 * <pre>{@code
 * @Test
 * void testCdc() throws Exception {
 *     // Setup
 *     Network network = Network.newNetwork();
 *     RedpandaContainer redpanda = createRedpandaContainer(network);
 *     PostgreSQLContainer postgres = createPostgresContainer(network);
 *     GenericContainer debezium = createDebeziumContainer(network, redpanda);
 *
 *     // Database setup
 *     Connection conn = getConnection(postgres);
 *     setupTestDatabase(conn);
 *     insertTestRecord(conn, 1, "Test Title");
 *
 *     // Register connector
 *     registerConnector(debezium, "test-connector", createConnectorConfig(postgres));
 *     waitForSnapshotCompletion(debezium, SNAPSHOT_TIMEOUT);
 *
 *     // Consume and validate
 *     KafkaConsumer<String, String> consumer = createConsumer(redpanda.getBootstrapServers());
 *     consumer.subscribe(List.of(getTestTopicName()));
 *     List<CdcEvent> events = toCdcEvents(drainEvents(consumer, 1, EVENT_CONSUMPTION_TIMEOUT));
 *
 *     CdcEvent event = events.get(0);
 *     assertEquals(1, event.getId());
 *     assertTrue(event.isReadOperation());
 *     assertEquals("Test Title", event.getTitle());
 * }
 * }</pre>
 */
package io.wcygan.testutil;
