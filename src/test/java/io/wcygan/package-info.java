/**
 * Integration tests demonstrating PostgreSQL CDC (Change Data Capture) using Debezium.
 *
 * <h2>Test Structure</h2>
 * <ul>
 *   <li>{@link io.wcygan.DebeziumIntegrationTest} - CDC test using Kafka and DebeziumContainer</li>
 *   <li>{@link io.wcygan.RedpandaDebeziumIntegrationTest} - CDC test using RedPanda with manual Debezium setup</li>
 *   <li>{@link io.wcygan.RedpandaConnectivityTest} - Basic RedPanda produce/consume validation</li>
 * </ul>
 *
 * <h2>Helper Classes</h2>
 * All common test utilities are located in {@link io.wcygan.testutil}:
 * <ul>
 *   <li>{@link io.wcygan.testutil.TestConstants} - Configuration constants and timeouts</li>
 *   <li>{@link io.wcygan.testutil.CdcEvent} - Value object for CDC event assertions</li>
 *   <li>{@link io.wcygan.testutil.CdcTestHelper} - CDC operations (database setup, connector registration)</li>
 *   <li>{@link io.wcygan.testutil.KafkaTestHelper} - Kafka consumer/producer utilities</li>
 *   <li>{@link io.wcygan.testutil.ContainerHelper} - TestContainers factory methods</li>
 * </ul>
 *
 * <h2>Testing Patterns</h2>
 * Tests follow a consistent pattern:
 * <ol>
 *   <li>Setup containers (RedPanda/Kafka, PostgreSQL, Debezium)</li>
 *   <li>Create database schema and insert test data</li>
 *   <li>Register Debezium connector</li>
 *   <li>Wait for snapshot completion (RedPanda tests only)</li>
 *   <li>Consume CDC events from Kafka/RedPanda topic</li>
 *   <li>Validate events using {@link io.wcygan.testutil.CdcEvent} assertions</li>
 * </ol>
 *
 * <h2>Key Differences</h2>
 * <table border="1">
 *   <tr>
 *     <th>Aspect</th>
 *     <th>DebeziumIntegrationTest</th>
 *     <th>RedpandaDebeziumIntegrationTest</th>
 *   </tr>
 *   <tr>
 *     <td>Messaging Platform</td>
 *     <td>Kafka (KafkaContainer)</td>
 *     <td>RedPanda (RedpandaContainer)</td>
 *   </tr>
 *   <tr>
 *     <td>Debezium Setup</td>
 *     <td>DebeziumContainer with .withKafka()</td>
 *     <td>GenericContainer with manual config</td>
 *   </tr>
 *   <tr>
 *     <td>Connector Registration</td>
 *     <td>debeziumContainer.registerConnector()</td>
 *     <td>HTTP API via CdcTestHelper</td>
 *   </tr>
 *   <tr>
 *     <td>Snapshot Wait</td>
 *     <td>Not required (handled by DebeziumContainer)</td>
 *     <td>Explicit wait via log polling</td>
 *   </tr>
 *   <tr>
 *     <td>Network Listeners</td>
 *     <td>Automatic</td>
 *     <td>Requires withListener() for dual listeners</td>
 *   </tr>
 * </table>
 *
 * @see <a href="https://debezium.io/documentation/">Debezium Documentation</a>
 * @see <a href="https://java.testcontainers.org/modules/redpanda/">TestContainers RedPanda Module</a>
 */
package io.wcygan;
