package io.wcygan.testutil;

import com.jayway.jsonpath.JsonPath;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Value object representing a Debezium CDC event.
 *
 * <p>Provides type-safe access to CDC event fields and eliminates JsonPath repetition in tests.
 */
public final class CdcEvent {

    private final String key;
    private final String value;

    private CdcEvent(String key, String value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Creates a CdcEvent from a Kafka ConsumerRecord.
     */
    public static CdcEvent from(ConsumerRecord<String, String> record) {
        return new CdcEvent(record.key(), record.value());
    }

    /**
     * Gets the ID from the event key.
     */
    public int getId() {
        Number id = JsonPath.read(key, "$.id");
        return id.intValue();
    }

    /**
     * Gets the operation type from the event value (c=create, u=update, d=delete, r=read/snapshot).
     */
    public String getOperation() {
        return JsonPath.read(value, "$.op");
    }

    /**
     * Checks if this is a read/snapshot operation.
     */
    public boolean isReadOperation() {
        return "r".equals(getOperation());
    }

    /**
     * Checks if this is a create operation.
     */
    public boolean isCreateOperation() {
        return "c".equals(getOperation());
    }

    /**
     * Checks if this is an update operation.
     */
    public boolean isUpdateOperation() {
        return "u".equals(getOperation());
    }

    /**
     * Checks if this is a delete operation.
     */
    public boolean isDeleteOperation() {
        return "d".equals(getOperation());
    }

    /**
     * Gets a field value from the "after" section of the event.
     */
    public <T> T getAfterField(String fieldPath, Class<T> type) {
        return JsonPath.read(value, "$.after." + fieldPath);
    }

    /**
     * Gets the title field from the "after" section.
     */
    public String getTitle() {
        return getAfterField("title", String.class);
    }

    /**
     * Gets a field value from the "before" section of the event.
     */
    public <T> T getBeforeField(String fieldPath, Class<T> type) {
        return JsonPath.read(value, "$.before." + fieldPath);
    }

    /**
     * Gets the raw key JSON.
     */
    public String getKey() {
        return key;
    }

    /**
     * Gets the raw value JSON.
     */
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "CdcEvent{id=" + getId() + ", op=" + getOperation() + "}";
    }
}
