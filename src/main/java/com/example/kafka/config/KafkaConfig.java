package com.example.kafka.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration for Custom Kafka library.
 * Provides a fluent builder API for easy configuration.
 */
public class KafkaConfig {

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final SchemaType schemaType;
    private final Map<String, Object> additionalProperties;

    // Producer-specific settings
    private final String acks;
    private final int retries;
    private final int retryBackoffMs;
    private final int deliveryTimeoutMs;
    private final boolean enableIdempotence;

    // Consumer-specific settings
    private final String groupId;
    private final String autoOffsetReset;
    private final boolean enableAutoCommit;
    private final int autoCommitIntervalMs;
    private final int maxPollRecords;

    private KafkaConfig(Builder builder) {
        this.bootstrapServers = builder.bootstrapServers;
        this.schemaRegistryUrl = builder.schemaRegistryUrl;
        this.schemaType = builder.schemaType;
        this.additionalProperties = builder.additionalProperties;
        this.acks = builder.acks;
        this.retries = builder.retries;
        this.retryBackoffMs = builder.retryBackoffMs;
        this.deliveryTimeoutMs = builder.deliveryTimeoutMs;
        this.enableIdempotence = builder.enableIdempotence;
        this.groupId = builder.groupId;
        this.autoOffsetReset = builder.autoOffsetReset;
        this.enableAutoCommit = builder.enableAutoCommit;
        this.autoCommitIntervalMs = builder.autoCommitIntervalMs;
        this.maxPollRecords = builder.maxPollRecords;
    }

    public static Builder builder() {
        return new Builder();
    }

    // Getters
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public SchemaType getSchemaType() {
        return schemaType;
    }

    public Map<String, Object> getAdditionalProperties() {
        return additionalProperties;
    }

    public String getAcks() {
        return acks;
    }

    public int getRetries() {
        return retries;
    }

    public int getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public int getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    public boolean isEnableIdempotence() {
        return enableIdempotence;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public boolean isEnableAutoCommit() {
        return enableAutoCommit;
    }

    public int getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    /**
     * Convert to Kafka producer properties.
     */
    public Properties toProducerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", acks);
        props.put("retries", retries);
        props.put("retry.backoff.ms", retryBackoffMs);
        props.put("delivery.timeout.ms", deliveryTimeoutMs);
        props.put("enable.idempotence", enableIdempotence);

        // Schema Registry
        props.put("schema.registry.url", schemaRegistryUrl);

        // Add additional properties
        additionalProperties.forEach(props::put);

        return props;
    }

    /**
     * Convert to Kafka consumer properties.
     */
    public Properties toConsumerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("auto.offset.reset", autoOffsetReset);
        props.put("enable.auto.commit", enableAutoCommit);
        props.put("auto.commit.interval.ms", autoCommitIntervalMs);
        props.put("max.poll.records", maxPollRecords);

        // Schema Registry
        props.put("schema.registry.url", schemaRegistryUrl);

        // Add additional properties
        additionalProperties.forEach(props::put);

        return props;
    }

    /**
     * Schema type enumeration.
     */
    public enum SchemaType {
        AVRO,
        JSON_SCHEMA
    }

    /**
     * Builder for KafkaConfig.
     */
    public static class Builder {
        private String bootstrapServers = "localhost:9092";
        private String schemaRegistryUrl = "http://localhost:8081";
        private SchemaType schemaType = SchemaType.AVRO;
        private Map<String, Object> additionalProperties = new HashMap<>();

        // Producer defaults - optimized for reliability
        private String acks = "all";
        private int retries = 3;
        private int retryBackoffMs = 1000;
        private int deliveryTimeoutMs = 120000;
        private boolean enableIdempotence = true;

        // Consumer defaults
        private String groupId = "custom-kafka-consumer";
        private String autoOffsetReset = "earliest";
        private boolean enableAutoCommit = false;
        private int autoCommitIntervalMs = 5000;
        private int maxPollRecords = 500;

        public Builder bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder schemaRegistryUrl(String schemaRegistryUrl) {
            this.schemaRegistryUrl = schemaRegistryUrl;
            return this;
        }

        public Builder schemaType(SchemaType schemaType) {
            this.schemaType = schemaType;
            return this;
        }

        public Builder additionalProperty(String key, Object value) {
            this.additionalProperties.put(key, value);
            return this;
        }

        public Builder additionalProperties(Map<String, Object> properties) {
            this.additionalProperties.putAll(properties);
            return this;
        }

        public Builder acks(String acks) {
            this.acks = acks;
            return this;
        }

        public Builder retries(int retries) {
            this.retries = retries;
            return this;
        }

        public Builder retryBackoffMs(int retryBackoffMs) {
            this.retryBackoffMs = retryBackoffMs;
            return this;
        }

        public Builder deliveryTimeoutMs(int deliveryTimeoutMs) {
            this.deliveryTimeoutMs = deliveryTimeoutMs;
            return this;
        }

        public Builder enableIdempotence(boolean enableIdempotence) {
            this.enableIdempotence = enableIdempotence;
            return this;
        }

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder autoOffsetReset(String autoOffsetReset) {
            this.autoOffsetReset = autoOffsetReset;
            return this;
        }

        public Builder enableAutoCommit(boolean enableAutoCommit) {
            this.enableAutoCommit = enableAutoCommit;
            return this;
        }

        public Builder autoCommitIntervalMs(int autoCommitIntervalMs) {
            this.autoCommitIntervalMs = autoCommitIntervalMs;
            return this;
        }

        public Builder maxPollRecords(int maxPollRecords) {
            this.maxPollRecords = maxPollRecords;
            return this;
        }

        public KafkaConfig build() {
            validate();
            return new KafkaConfig(this);
        }

        private void validate() {
            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                throw new IllegalArgumentException("bootstrapServers is required");
            }
            if (schemaRegistryUrl == null || schemaRegistryUrl.isEmpty()) {
                throw new IllegalArgumentException("schemaRegistryUrl is required");
            }
        }
    }
}


