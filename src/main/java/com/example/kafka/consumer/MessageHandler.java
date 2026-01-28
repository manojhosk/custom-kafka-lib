package com.example.kafka.consumer;

/**
 * Functional interface for handling consumed messages.
 *
 * @param <V> The type of the message value
 */
@FunctionalInterface
public interface MessageHandler<V> {

    /**
     * Handle a consumed message.
     *
     * @param message The consumed message
     * @throws Exception if handling fails (message will be retried or sent to DLQ)
     */
    void handle(ConsumedMessage<V> message) throws Exception;
}

