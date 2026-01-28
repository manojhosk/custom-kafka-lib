package com.example.kafka.exception;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for custom exceptions.
 */
class ExceptionTest {

    @Test
    @DisplayName("SchemaValidationException should contain validation errors")
    void schemaValidationExceptionShouldContainErrors() {
        String errors = "Field 'name' is required; Field 'age' must be positive";
        SchemaValidationException exception = new SchemaValidationException(
                "Validation failed",
                "{\"type\": \"object\"}",
                errors
        );

        assertEquals("Validation failed", exception.getMessage());
        assertTrue(exception.getValidationErrors().contains("Field 'name' is required"));
        assertEquals("{\"type\": \"object\"}", exception.getSchemaId());
    }

    @Test
    @DisplayName("ProducerException should contain topic information")
    void producerExceptionShouldContainTopicInfo() {
        ProducerException exception = new ProducerException(
                "Failed to send message",
                "users-topic"
        );

        assertEquals("Failed to send message", exception.getMessage());
        assertEquals("users-topic", exception.getTopic());
    }

    @Test
    @DisplayName("ProducerException should contain cause")
    void producerExceptionShouldContainCause() {
        RuntimeException cause = new RuntimeException("Network error");
        ProducerException exception = new ProducerException(
                "Failed to send message",
                "users-topic",
                cause
        );

        assertEquals(cause, exception.getCause());
        assertEquals("users-topic", exception.getTopic());
    }

    @Test
    @DisplayName("ConsumerException should contain topic information")
    void consumerExceptionShouldContainTopicInfo() {
        ConsumerException exception = new ConsumerException(
                "Failed to consume message",
                "orders-topic"
        );

        assertEquals("Failed to consume message", exception.getMessage());
        assertEquals("orders-topic", exception.getTopic());
    }

    @Test
    @DisplayName("SchemaRegistryException should contain subject information")
    void schemaRegistryExceptionShouldContainSubject() {
        SchemaRegistryException exception = new SchemaRegistryException(
                "Failed to register schema",
                "users-value"
        );

        assertEquals("Failed to register schema", exception.getMessage());
        assertEquals("users-value", exception.getSubject());
    }

    @Test
    @DisplayName("SchemaRegistryException should contain cause")
    void schemaRegistryExceptionShouldContainCause() {
        RuntimeException cause = new RuntimeException("Connection refused");
        SchemaRegistryException exception = new SchemaRegistryException(
                "Failed to register schema",
                "users-value",
                cause
        );

        assertEquals(cause, exception.getCause());
        assertEquals("users-value", exception.getSubject());
    }

    @Test
    @DisplayName("CustomKafkaException should be base of all exceptions")
    void customKafkaExceptionShouldBeBase() {
        CustomKafkaException producerEx = new ProducerException("test", "topic");
        CustomKafkaException consumerEx = new ConsumerException("test", "topic");
        CustomKafkaException registryEx = new SchemaRegistryException("test", "subject");
        CustomKafkaException validationEx = new SchemaValidationException("test", "schema", "error");

        assertInstanceOf(CustomKafkaException.class, producerEx);
        assertInstanceOf(CustomKafkaException.class, consumerEx);
        assertInstanceOf(CustomKafkaException.class, registryEx);
        assertInstanceOf(CustomKafkaException.class, validationEx);
    }
}

