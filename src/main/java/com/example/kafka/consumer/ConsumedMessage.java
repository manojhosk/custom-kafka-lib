package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

/**
 * Represents a consumed message from Kafka.
 *
 * @param <V> The type of the message value
 */
public class ConsumedMessage<V> {

    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final String key;
    private final V value;
    private final Map<String, String> headers;

    public ConsumedMessage(String topic, int partition, long offset, long timestamp,
                          String key, V value, Map<String, String> headers) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.headers = headers;
    }

    /**
     * Create a ConsumedMessage from a Kafka ConsumerRecord.
     */
    public static <V> ConsumedMessage<V> from(ConsumerRecord<String, V> record) {
        Map<String, String> headers = new java.util.HashMap<>();
        record.headers().forEach(header ->
            headers.put(header.key(), new String(header.value(), java.nio.charset.StandardCharsets.UTF_8)));

        return new ConsumedMessage<>(
            record.topic(),
            record.partition(),
            record.offset(),
            record.timestamp(),
            record.key(),
            record.value(),
            headers
        );
    }

    // Getters
    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public String toString() {
        return String.format("ConsumedMessage{topic='%s', partition=%d, offset=%d, timestamp=%d, key='%s', value=%s}",
            topic, partition, offset, timestamp, key, value);
    }
}

