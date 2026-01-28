package com.example.kafka.exception;

/**
 * Base exception for all Custom Kafka library exceptions.
 */
public class CustomKafkaException extends RuntimeException {

    public CustomKafkaException(String message) {
        super(message);
    }

    public CustomKafkaException(String message, Throwable cause) {
        super(message, cause);
    }
}

