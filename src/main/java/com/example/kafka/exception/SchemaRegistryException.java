package com.example.kafka.exception;

/**
 * Exception thrown when schema registry operations fail.
 */
public class SchemaRegistryException extends CustomKafkaException {

    private final String subject;

    public SchemaRegistryException(String message, String subject) {
        super(message);
        this.subject = subject;
    }

    public SchemaRegistryException(String message, String subject, Throwable cause) {
        super(message, cause);
        this.subject = subject;
    }

    public String getSubject() {
        return subject;
    }
}

