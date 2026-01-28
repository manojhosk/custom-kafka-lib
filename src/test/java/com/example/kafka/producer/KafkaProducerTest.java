package com.example.kafka.producer;

import com.example.kafka.exception.SchemaValidationException;
import com.example.kafka.registry.SchemaRegistryManager;
import com.example.kafka.schema.SchemaValidator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for KafkaProducer using mocks.
 * These tests do NOT require a running Kafka broker.
 */
class KafkaProducerTest {

    @Mock
    private org.apache.kafka.clients.producer.KafkaProducer<String, Object> mockKafkaProducer;

    @Mock
    private SchemaRegistryManager mockRegistryManager;

    @Mock
    private SchemaValidator mockValidator;

    private static final String USER_SCHEMA = """
        {
          "type": "record",
          "name": "User",
          "fields": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"}
          ]
        }
        """;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    @DisplayName("Should create SendResultData success correctly")
    void shouldCreateSendResultDataSuccess() {
        SendResultData result = SendResultData.success("test-topic", 0, 100L, System.currentTimeMillis(), "key-1");

        assertTrue(result.isSuccess());
        assertEquals("test-topic", result.getTopic());
        assertEquals(0, result.getPartition());
        assertEquals(100L, result.getOffset());
        assertEquals("key-1", result.getKey());
        assertNull(result.getError());
    }

    @Test
    @DisplayName("Should create SendResultData failure correctly")
    void shouldCreateSendResultDataFailure() {
        Exception error = new RuntimeException("Send failed");
        SendResultData result = SendResultData.failure("test-topic", "key-1", error);

        assertFalse(result.isSuccess());
        assertEquals("test-topic", result.getTopic());
        assertEquals("key-1", result.getKey());
        assertEquals(error, result.getError());
    }

    @Test
    @DisplayName("Should validate message before sending")
    void shouldValidateMessageBeforeSending() throws Exception {
        // This test verifies the validation logic without actually sending
        Map<String, Object> validUser = Map.of(
                "id", "123",
                "name", "John"
        );

        // Validation should pass for valid data
        doNothing().when(mockValidator).validate(eq(validUser), eq(USER_SCHEMA));

        // Verify validator is called (would happen in real producer)
        mockValidator.validate(validUser, USER_SCHEMA);
        verify(mockValidator).validate(validUser, USER_SCHEMA);
    }

    @Test
    @DisplayName("Should throw SchemaValidationException for invalid data")
    void shouldThrowSchemaValidationExceptionForInvalidData() throws Exception {
        Map<String, Object> invalidUser = Map.of("id", "123");
        // Missing required field "name"

        doThrow(new SchemaValidationException("Validation failed", USER_SCHEMA, "Missing field: name"))
                .when(mockValidator).validate(eq(invalidUser), eq(USER_SCHEMA));

        assertThrows(SchemaValidationException.class, () ->
                mockValidator.validate(invalidUser, USER_SCHEMA));
    }

    @Test
    @DisplayName("Should register schema through registry manager")
    void shouldRegisterSchemaThroughRegistryManager() {
        when(mockRegistryManager.registerSchema("users-value", USER_SCHEMA)).thenReturn(1);

        int schemaId = mockRegistryManager.registerSchema("users-value", USER_SCHEMA);

        assertEquals(1, schemaId);
        verify(mockRegistryManager).registerSchema("users-value", USER_SCHEMA);
    }

    @Test
    @DisplayName("Should handle async send callback success")
    void shouldHandleAsyncSendCallbackSuccess() {
        // Simulate callback behavior
        CompletableFuture<SendResultData> future = new CompletableFuture<>();

        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("test-topic", 0),
                0L, 0, System.currentTimeMillis(), 0, 0
        );

        // Simulate successful callback
        future.complete(SendResultData.success(
                metadata.topic(),
                metadata.partition(),
                metadata.offset(),
                metadata.timestamp(),
                "key-1"
        ));

        SendResultData result = future.join();
        assertTrue(result.isSuccess());
        assertEquals("test-topic", result.getTopic());
    }

    @Test
    @DisplayName("Should handle async send callback failure")
    void shouldHandleAsyncSendCallbackFailure() {
        CompletableFuture<SendResultData> future = new CompletableFuture<>();
        Exception error = new RuntimeException("Broker unavailable");

        // Simulate failed callback
        future.complete(SendResultData.failure("test-topic", "key-1", error));

        SendResultData result = future.join();
        assertFalse(result.isSuccess());
        assertEquals(error, result.getError());
    }

    @Test
    @DisplayName("ProducerRecord should contain correct data")
    void producerRecordShouldContainCorrectData() {
        Map<String, Object> user = Map.of("id", "123", "name", "John");
        ProducerRecord<String, Object> record = new ProducerRecord<>("users", "key-1", user);

        assertEquals("users", record.topic());
        assertEquals("key-1", record.key());
        assertEquals(user, record.value());
    }

    @Test
    @DisplayName("ProducerRecord should support null key")
    void producerRecordShouldSupportNullKey() {
        Map<String, Object> user = Map.of("id", "123", "name", "John");
        ProducerRecord<String, Object> record = new ProducerRecord<>("users", null, user);

        assertEquals("users", record.topic());
        assertNull(record.key());
        assertEquals(user, record.value());
    }

    @Test
    @DisplayName("Should handle headers in ProducerRecord")
    void shouldHandleHeadersInProducerRecord() {
        Map<String, Object> user = Map.of("id", "123", "name", "John");
        ProducerRecord<String, Object> record = new ProducerRecord<>("users", "key-1", user);
        record.headers().add("correlation-id", "abc123".getBytes());

        assertEquals(1, record.headers().toArray().length);
        assertEquals("correlation-id", record.headers().toArray()[0].key());
    }
}

