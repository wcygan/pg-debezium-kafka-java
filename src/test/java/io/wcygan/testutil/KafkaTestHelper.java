package io.wcygan.testutil;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Helper utilities for Kafka operations in tests.
 */
public final class KafkaTestHelper {

    private KafkaTestHelper() {
        // Utility class
    }

    /**
     * Creates a Kafka consumer configured for testing.
     *
     * @param bootstrapServers Kafka bootstrap servers address
     * @return configured KafkaConsumer
     */
    public static KafkaConsumer<String, String> createConsumer(String bootstrapServers) {
        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(),
                new StringDeserializer());
    }

    /**
     * Drains events from a Kafka consumer until the expected count is reached or timeout occurs.
     *
     * @param consumer the Kafka consumer
     * @param expectedCount number of events to wait for
     * @param timeout maximum time to wait
     * @return list of consumed records
     * @throws AssertionError if expected count not reached within timeout
     */
    public static List<ConsumerRecord<String, String>> drainEvents(
            KafkaConsumer<String, String> consumer,
            int expectedCount,
            Duration timeout) {

        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();

        // First poll with longer timeout to allow partition assignment
        consumer.poll(TestConstants.POLL_TIMEOUT_FIRST).forEach(records::add);

        while (System.nanoTime() < deadline && records.size() < expectedCount) {
            consumer.poll(TestConstants.POLL_TIMEOUT).forEach(records::add);
        }

        if (records.size() < expectedCount) {
            throw new AssertionError(
                    "Expected " + expectedCount + " change events but received " + records.size());
        }

        return records;
    }

    /**
     * Converts consumer records to CdcEvent objects.
     *
     * @param records list of consumer records
     * @return list of CdcEvent objects
     */
    public static List<CdcEvent> toCdcEvents(List<ConsumerRecord<String, String>> records) {
        return records.stream()
                .map(CdcEvent::from)
                .toList();
    }
}
