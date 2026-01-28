package com.example.kafka.exception;

/**
 * Exception thrown when message consumption fails.
 */
public class ConsumerException extends CustomKafkaException {

    private final String topic;

    public ConsumerException(String message, String topic) {
        super(message);
        this.topic = topic;
    }

    public ConsumerException(String message, String topic, Throwable cause) {
        super(message, cause);
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}

