package com.example.kafka.producer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SendResultData.
 */
class SendResultDataTest {

    @Test
    @DisplayName("Should create successful result")
    void shouldCreateSuccessfulResult() {
        SendResultData result = SendResultData.success(
                "test-topic",
                0,
                100L,
                System.currentTimeMillis(),
                "test-key"
        );

        assertTrue(result.isSuccess());
        assertEquals("test-topic", result.getTopic());
        assertEquals(0, result.getPartition());
        assertEquals(100L, result.getOffset());
        assertEquals("test-key", result.getKey());
        assertNull(result.getError());
    }

    @Test
    @DisplayName("Should create failure result")
    void shouldCreateFailureResult() {
        RuntimeException error = new RuntimeException("Test error");
        SendResultData result = SendResultData.failure("test-topic", "test-key", error);

        assertFalse(result.isSuccess());
        assertEquals("test-topic", result.getTopic());
        assertEquals("test-key", result.getKey());
        assertEquals(error, result.getError());
    }

    @Test
    @DisplayName("Should create result using builder")
    void shouldCreateResultUsingBuilder() {
        long timestamp = System.currentTimeMillis();
        SendResultData result = SendResultData.builder()
                .topic("my-topic")
                .partition(2)
                .offset(500L)
                .timestamp(timestamp)
                .key("my-key")
                .success(true)
                .build();

        assertTrue(result.isSuccess());
        assertEquals("my-topic", result.getTopic());
        assertEquals(2, result.getPartition());
        assertEquals(500L, result.getOffset());
        assertEquals(timestamp, result.getTimestamp());
        assertEquals("my-key", result.getKey());
    }

    @Test
    @DisplayName("Should handle null key in success result")
    void shouldHandleNullKeyInSuccessResult() {
        SendResultData result = SendResultData.success(
                "test-topic",
                0,
                100L,
                System.currentTimeMillis(),
                null
        );

        assertTrue(result.isSuccess());
        assertNull(result.getKey());
    }

    @Test
    @DisplayName("Should handle null key in failure result")
    void shouldHandleNullKeyInFailureResult() {
        SendResultData result = SendResultData.failure(
                "test-topic",
                null,
                new RuntimeException("Error")
        );

        assertFalse(result.isSuccess());
        assertNull(result.getKey());
    }

    @Test
    @DisplayName("Should have meaningful toString")
    void shouldHaveMeaningfulToString() {
        SendResultData result = SendResultData.success(
                "test-topic",
                0,
                100L,
                1234567890L,
                "test-key"
        );

        String toString = result.toString();
        assertTrue(toString.contains("test-topic"));
        assertTrue(toString.contains("test-key"));
    }
}

