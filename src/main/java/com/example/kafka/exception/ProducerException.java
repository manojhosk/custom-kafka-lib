package com.example.kafka.exception;

/**
 * Exception thrown when message production fails.
 */
public class ProducerException extends CustomKafkaException {

    private final String topic;

    public ProducerException(String message, String topic) {
        super(message);
        this.topic = topic;
    }

    public ProducerException(String message, String topic, Throwable cause) {
        super(message, cause);
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}

