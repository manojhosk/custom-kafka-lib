package com.example.kafka.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KafkaConfig.
 */
class KafkaConfigTest {

    @Test
    @DisplayName("Should build config with default values")
    void shouldBuildConfigWithDefaults() {
        KafkaConfig config = KafkaConfig.builder().build();

        assertEquals("localhost:9092", config.getBootstrapServers());
        assertEquals("http://localhost:8081", config.getSchemaRegistryUrl());
        assertEquals(KafkaConfig.SchemaType.AVRO, config.getSchemaType());
        assertEquals("custom-kafka-consumer", config.getGroupId());
        assertEquals("earliest", config.getAutoOffsetReset());
        assertFalse(config.isEnableAutoCommit());
        assertEquals(3, config.getRetries());
        assertTrue(config.isEnableIdempotence());
        assertEquals("all", config.getAcks());
    }

    @Test
    @DisplayName("Should build config with custom values")
    void shouldBuildConfigWithCustomValues() {
        KafkaConfig config = KafkaConfig.builder()
                .bootstrapServers("kafka1:9092,kafka2:9092")
                .schemaRegistryUrl("http://registry:8081")
                .schemaType(KafkaConfig.SchemaType.JSON_SCHEMA)
                .groupId("my-group")
                .autoOffsetReset("latest")
                .enableAutoCommit(true)
                .retries(5)
                .retryBackoffMs(2000)
                .deliveryTimeoutMs(60000)
                .enableIdempotence(false)
                .acks("1")
                .maxPollRecords(100)
                .build();

        assertEquals("kafka1:9092,kafka2:9092", config.getBootstrapServers());
        assertEquals("http://registry:8081", config.getSchemaRegistryUrl());
        assertEquals(KafkaConfig.SchemaType.JSON_SCHEMA, config.getSchemaType());
        assertEquals("my-group", config.getGroupId());
        assertEquals("latest", config.getAutoOffsetReset());
        assertTrue(config.isEnableAutoCommit());
        assertEquals(5, config.getRetries());
        assertEquals(2000, config.getRetryBackoffMs());
        assertEquals(60000, config.getDeliveryTimeoutMs());
        assertFalse(config.isEnableIdempotence());
        assertEquals("1", config.getAcks());
        assertEquals(100, config.getMaxPollRecords());
    }

    @Test
    @DisplayName("Should add additional properties")
    void shouldAddAdditionalProperties() {
        KafkaConfig config = KafkaConfig.builder()
                .additionalProperty("custom.key", "custom.value")
                .additionalProperties(Map.of("another.key", "another.value"))
                .build();

        Map<String, Object> props = config.getAdditionalProperties();
        assertEquals("custom.value", props.get("custom.key"));
        assertEquals("another.value", props.get("another.key"));
    }

    @Test
    @DisplayName("Should throw exception when bootstrapServers is null")
    void shouldThrowExceptionWhenBootstrapServersIsNull() {
        assertThrows(IllegalArgumentException.class, () ->
                KafkaConfig.builder()
                        .bootstrapServers(null)
                        .build()
        );
    }

    @Test
    @DisplayName("Should throw exception when bootstrapServers is empty")
    void shouldThrowExceptionWhenBootstrapServersIsEmpty() {
        assertThrows(IllegalArgumentException.class, () ->
                KafkaConfig.builder()
                        .bootstrapServers("")
                        .build()
        );
    }

    @Test
    @DisplayName("Should throw exception when schemaRegistryUrl is null")
    void shouldThrowExceptionWhenSchemaRegistryUrlIsNull() {
        assertThrows(IllegalArgumentException.class, () ->
                KafkaConfig.builder()
                        .schemaRegistryUrl(null)
                        .build()
        );
    }

    @Test
    @DisplayName("Should generate valid producer properties")
    void shouldGenerateValidProducerProperties() {
        KafkaConfig config = KafkaConfig.builder()
                .bootstrapServers("localhost:9092")
                .schemaRegistryUrl("http://localhost:8081")
                .retries(5)
                .acks("all")
                .enableIdempotence(true)
                .build();

        Properties props = config.toProducerProperties();

        assertEquals("localhost:9092", props.get("bootstrap.servers"));
        assertEquals("http://localhost:8081", props.get("schema.registry.url"));
        assertEquals("5", props.get("retries").toString());
        assertEquals("all", props.get("acks"));
        assertEquals("true", props.get("enable.idempotence").toString());
    }

    @Test
    @DisplayName("Should generate valid consumer properties")
    void shouldGenerateValidConsumerProperties() {
        KafkaConfig config = KafkaConfig.builder()
                .bootstrapServers("localhost:9092")
                .schemaRegistryUrl("http://localhost:8081")
                .groupId("test-group")
                .autoOffsetReset("earliest")
                .enableAutoCommit(false)
                .maxPollRecords(200)
                .build();

        Properties props = config.toConsumerProperties();

        assertEquals("localhost:9092", props.get("bootstrap.servers"));
        assertEquals("http://localhost:8081", props.get("schema.registry.url"));
        assertEquals("test-group", props.get("group.id"));
        assertEquals("earliest", props.get("auto.offset.reset"));
        assertEquals("false", props.get("enable.auto.commit").toString());
        assertEquals("200", props.get("max.poll.records").toString());
    }
}

