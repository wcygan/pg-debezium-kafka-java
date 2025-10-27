# RedPanda CDC Integration Fix

## Problem

PostgreSQL CDC integration test using Debezium and RedPanda was failing with 0 CDC events consumed despite:
- All containers starting successfully
- Debezium snapshot completing (2 records exported)
- Messages existing in RedPanda topic (verified via rpk CLI)

## Root Cause

**Container-to-container network communication issue with RedPanda advertised listeners**

When using a custom Docker network with TestContainers:
- Debezium Connect runs inside the network and needs to connect to RedPanda via network alias (`redpanda:9092`)
- Test consumer runs on the host and needs to connect via dynamically mapped port (`localhost:xxxxx`)
- RedPanda's default listener configuration doesn't support both scenarios simultaneously

## Solution

Use `RedpandaContainer.withListener()` to configure dual listeners for both internal and external connectivity:

```java
private final RedpandaContainer redpandaContainer =
        new RedpandaContainer(DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.3.3"))
                .withListener(() -> "redpanda:19092")  // Internal listener for Debezium
                .withNetwork(network)
                .withNetworkAliases("redpanda");
```

Then configure consumers appropriately:

**Debezium (internal):**
```java
.withEnv("BOOTSTRAP_SERVERS", "redpanda:19092")  // Network alias + listener port
```

**Test consumer (external):**
```java
ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, redpandaContainer.getBootstrapServers()  // Dynamic mapped port
```

## Key Changes

### RedpandaDebeziumIntegrationTest.java

**Before (broken):**
```java
private final GenericContainer<?> redpandaContainer =
        new GenericContainer<>(DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.3.3"))
                .withNetwork(network)
                .withNetworkAliases("redpanda")
                .withCommand(
                        "redpanda",
                        "start",
                        "--mode=dev-container",
                        "--kafka-addr=internal://0.0.0.0:9092,external://0.0.0.0:19092",
                        "--advertise-kafka-addr=internal://redpanda:9092,external://localhost:19092")
                .withExposedPorts(9092, 19092);

private final GenericContainer<?> debeziumContainer =
        new GenericContainer<>(...)
                .withEnv("BOOTSTRAP_SERVERS", "redpanda:9092");  // Couldn't connect

private KafkaConsumer<String, String> createConsumer() {
    String bootstrapServers = redpandaContainer.getHost() + ":" + redpandaContainer.getMappedPort(19092);
    return new KafkaConsumer<>(...);  // Redirected to inaccessible address
}
```

**After (working):**
```java
private final RedpandaContainer redpandaContainer =
        new RedpandaContainer(DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.3.3"))
                .withListener(() -> "redpanda:19092")  // Add internal listener
                .withNetwork(network)
                .withNetworkAliases("redpanda");

private final GenericContainer<?> debeziumContainer =
        new GenericContainer<>(...)
                .withEnv("BOOTSTRAP_SERVERS", "redpanda:19092");  // Internal listener

private KafkaConsumer<String, String> createConsumer() {
    return new KafkaConsumer<>(
            Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                   redpandaContainer.getBootstrapServers(), ...));  // External listener
}
```

### Additional Fixes

1. **Schema-less JSON configuration:**
   ```java
   .withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false")
   .withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
   ```
   Note the `CONNECT_` prefix required for Kafka Connect environment variables.

2. **Snapshot completion wait:**
   ```java
   private void waitForSnapshotCompletion(Duration timeout) throws InterruptedException {
       long deadline = System.nanoTime() + timeout.toNanos();
       while (System.nanoTime() < deadline) {
           String logs = debeziumContainer.getLogs();
           if (logs.contains("Snapshot") && logs.contains("completed")) {
               return;
           }
           Thread.sleep(500);
       }
       throw new AssertionError("Snapshot did not complete within timeout");
   }
   ```

## References

- [TestContainers RedPanda Module](https://java.testcontainers.org/modules/redpanda/)
- [GitHub Issue #6395: RedPandaContainer container-to-container communication](https://github.com/testcontainers/testcontainers-java/issues/6395)
- [Debezium TestContainers Integration](https://debezium.io/documentation/reference/stable/integrations/testcontainers.html)

## Test Results

All tests pass after fix:
- `RedpandaDebeziumIntegrationTest`: 20.12s ✅
- `DebeziumIntegrationTest`: 15.48s ✅
- `RedpandaConnectivityTest`: 2.38s ✅

Total: 37.98s for complete CDC pipeline validation.
