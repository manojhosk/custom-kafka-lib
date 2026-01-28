package com.example.kafka.consumer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for HttpForwarder.Builder.
 */
class HttpForwarderTest {

    @Test
    @DisplayName("Should build forwarder with default values")
    void shouldBuildForwarderWithDefaults() {
        HttpForwarder forwarder = HttpForwarder.builder("http://localhost:8080/api")
                .build();

        assertNotNull(forwarder);
    }

    @Test
    @DisplayName("Should build forwarder with custom retry settings")
    void shouldBuildForwarderWithCustomRetrySettings() {
        HttpForwarder forwarder = HttpForwarder.builder("http://localhost:8080/api")
                .maxRetries(5)
                .retryDelayMs(2000)
                .build();

        assertNotNull(forwarder);
    }

    @Test
    @DisplayName("Should build forwarder with custom timeout settings")
    void shouldBuildForwarderWithCustomTimeoutSettings() {
        HttpForwarder forwarder = HttpForwarder.builder("http://localhost:8080/api")
                .connectTimeoutMs(10000)
                .readTimeoutMs(60000)
                .build();

        assertNotNull(forwarder);
    }

    @Test
    @DisplayName("Should build forwarder with custom headers")
    void shouldBuildForwarderWithCustomHeaders() {
        HttpForwarder forwarder = HttpForwarder.builder("http://localhost:8080/api")
                .header("Authorization", "Bearer token123")
                .header("X-Custom-Header", "custom-value")
                .build();

        assertNotNull(forwarder);
    }

    @Test
    @DisplayName("Should throw exception for null URL")
    void shouldThrowExceptionForNullUrl() {
        assertThrows(IllegalArgumentException.class, () ->
                HttpForwarder.builder(null).build()
        );
    }

    @Test
    @DisplayName("Should throw exception for empty URL")
    void shouldThrowExceptionForEmptyUrl() {
        assertThrows(IllegalArgumentException.class, () ->
                HttpForwarder.builder("").build()
        );
    }

    @Test
    @DisplayName("Should create message handler from forwarder")
    void shouldCreateMessageHandlerFromForwarder() {
        HttpForwarder forwarder = HttpForwarder.builder("http://localhost:8080/api")
                .build();

        MessageHandler<Object> handler = forwarder.asMessageHandler();
        assertNotNull(handler);
    }

    @Test
    @DisplayName("Should close forwarder without error")
    void shouldCloseForwarderWithoutError() throws Exception {
        HttpForwarder forwarder = HttpForwarder.builder("http://localhost:8080/api")
                .build();

        assertDoesNotThrow(forwarder::close);
    }
}

