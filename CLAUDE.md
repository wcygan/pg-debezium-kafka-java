# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PostgreSQL CDC (Change Data Capture) demonstration using Debezium and Kafka. This is a test-focused project that validates streaming database changes from PostgreSQL through Debezium into Kafka topics using TestContainers.

## Architecture

**Integration Test Pattern**: The main functionality lives in `DebeziumIntegrationTest.java` which orchestrates:
- PostgreSQL container with Debezium-compatible image (quay.io/debezium/postgres:18)
- Kafka container for message streaming
- Debezium Connect container to capture CDC events
- All containers share a Docker network for communication

**CDC Flow**: PostgreSQL → Debezium Connector → Kafka Topics → Consumer verification

**Key Components**:
- `App.java`: Minimal entry point (placeholder)
- `DebeziumIntegrationTest.java`: Full integration test demonstrating CDC pipeline
  - Uses `@TestInstance(Lifecycle.PER_CLASS)` to start containers once per test class
  - `drain()` method polls Kafka consumer until expected records arrive or timeout
  - JsonPath for extracting fields from Debezium JSON payloads

## Development Commands

### Build & Compile
```bash
mvn clean compile
```

### Run Tests
```bash
# Run all tests
mvn test

# Run specific test
mvn test -Dtest=DebeziumIntegrationTest

# Run specific test method
mvn test -Dtest=DebeziumIntegrationTest#postgresConnectorCapturesChanges
```

### Run Application
```bash
mvn exec:java
```

### Dependencies
```bash
# Show dependency tree
mvn dependency:tree

# Update dependencies
mvn versions:display-dependency-updates
```

## TestContainers Setup

**Container Lifecycle**: Containers start in `@BeforeAll` and stop in `@AfterAll`

**Network Configuration**: Shared Docker network enables containers to reference each other by network aliases (e.g., "postgres")

**PostgreSQL Configuration**: Tables must have `replica identity full` for Debezium to capture full row changes

**Debezium Connector**: Registered via REST API using `ConnectorConfiguration.forJdbcContainer()`

## Testing Patterns

**Integration Test Structure**:
1. Start containers (PostgreSQL, Kafka, Debezium)
2. Execute DDL and DML operations
3. Register Debezium connector
4. Subscribe to Kafka topics
5. Poll and verify CDC events
6. Stop containers

**Kafka Consumer Pattern**: Use `drain()` helper to poll until expected record count is received within timeout

**CDC Event Validation**: Use JsonPath to extract and assert on Debezium event structure:
- `$.op`: Operation type ("r" for read/snapshot, "c" for create, "u" for update, "d" for delete)
- `$.after`: New row state
- `$.before`: Previous row state (for updates/deletes)
- Key fields in separate `$.id` structure

## Java & Maven

- Java 21 (source/target)
- Maven Surefire for test execution
- TestContainers 1.19.7
- Debezium 3.3.1.Final
- Kafka 3.7.0
