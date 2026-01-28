package com.example.kafka.consumer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ConsumedMessage.
 */
class ConsumedMessageTest {

    @Test
    @DisplayName("Should create message with all fields")
    void shouldCreateMessageWithAllFields() {
        ConsumedMessage<String> message = new ConsumedMessage<>(
                "test-topic",
                0,
                100L,
                System.currentTimeMillis(),
                "test-key",
                "test-value",
                Map.of("header1", "value1")
        );

        assertEquals("test-topic", message.getTopic());
        assertEquals(0, message.getPartition());
        assertEquals(100L, message.getOffset());
        assertEquals("test-key", message.getKey());
        assertEquals("test-value", message.getValue());
        assertEquals("value1", message.getHeaders().get("header1"));
    }

    @Test
    @DisplayName("Should handle null key")
    void shouldHandleNullKey() {
        ConsumedMessage<String> message = new ConsumedMessage<>(
                "test-topic",
                0,
                100L,
                System.currentTimeMillis(),
                null,
                "test-value",
                Map.of()
        );

        assertNull(message.getKey());
        assertEquals("test-value", message.getValue());
    }

    @Test
    @DisplayName("Should handle empty headers")
    void shouldHandleEmptyHeaders() {
        ConsumedMessage<String> message = new ConsumedMessage<>(
                "test-topic",
                0,
                100L,
                System.currentTimeMillis(),
                "key",
                "value",
                Map.of()
        );

        assertTrue(message.getHeaders().isEmpty());
    }

    @Test
    @DisplayName("Should store timestamp correctly")
    void shouldStoreTimestampCorrectly() {
        long timestamp = System.currentTimeMillis();
        ConsumedMessage<String> message = new ConsumedMessage<>(
                "test-topic",
                0,
                100L,
                timestamp,
                "key",
                "value",
                Map.of()
        );

        assertEquals(timestamp, message.getTimestamp());
    }

    @Test
    @DisplayName("Should handle complex value types")
    void shouldHandleComplexValueTypes() {
        Map<String, Object> complexValue = Map.of(
                "id", "123",
                "name", "Test",
                "count", 42
        );

        ConsumedMessage<Map<String, Object>> message = new ConsumedMessage<>(
                "test-topic",
                0,
                100L,
                System.currentTimeMillis(),
                "key",
                complexValue,
                Map.of()
        );

        assertEquals(complexValue, message.getValue());
        assertEquals("123", message.getValue().get("id"));
    }
}

