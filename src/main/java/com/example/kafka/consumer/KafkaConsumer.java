package com.example.kafka.consumer;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.exception.ConsumerException;
import com.example.kafka.registry.SchemaRegistryManager;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * High-level Kafka consumer with schema support.
 * Provides a simple API for consuming schema-validated messages from Kafka.
 */
public class KafkaConsumer implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private final KafkaConfig config;
    private final org.apache.kafka.clients.consumer.KafkaConsumer<String, Object> consumer;
    private final SchemaRegistryManager schemaRegistryManager;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ExecutorService executorService;

    private MessageHandler<Object> messageHandler;
    private ErrorHandler errorHandler;
    private HttpForwarder httpForwarder;

    /**
     * Create a new KongKafkaConsumer with the given configuration.
     *
     * @param config The Kafka configuration
     */
    public KafkaConsumer(KafkaConfig config) {
        this.config = config;
        this.schemaRegistryManager = new SchemaRegistryManager(config);
        this.consumer = createConsumer();
        this.executorService = Executors.newSingleThreadExecutor();
        this.errorHandler = new DefaultErrorHandler();

        logger.info("KongKafkaConsumer initialized with group ID: {}", config.getGroupId());
    }

    private org.apache.kafka.clients.consumer.KafkaConsumer<String, Object> createConsumer() {
        Properties props = config.toConsumerProperties();

        // Key deserializer is always String
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Value deserializer depends on schema type
        if (config.getSchemaType() == KafkaConfig.SchemaType.AVRO) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
            props.put("specific.avro.reader", "false"); // Use GenericRecord
        } else {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        }

        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
    }

    /**
     * Subscribe to topics and start consuming messages.
     *
     * @param topics List of topics to subscribe to
     * @param handler The message handler
     */
    public void subscribe(List<String> topics, MessageHandler<Object> handler) {
        this.messageHandler = handler;
        consumer.subscribe(topics);
        logger.info("Subscribed to topics: {}", topics);
    }

    /**
     * Subscribe to a single topic.
     *
     * @param topic The topic to subscribe to
     * @param handler The message handler
     */
    public void subscribe(String topic, MessageHandler<Object> handler) {
        subscribe(Collections.singletonList(topic), handler);
    }

    /**
     * Configure the consumer to forward messages to an HTTP endpoint.
     *
     * @param forwarder The HTTP forwarder
     * @return This consumer for chaining
     */
    public KafkaConsumer forwardTo(HttpForwarder forwarder) {
        this.httpForwarder = forwarder;
        return this;
    }

    /**
     * Set a custom error handler.
     *
     * @param errorHandler The error handler
     * @return This consumer for chaining
     */
    public KafkaConsumer withErrorHandler(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
        return this;
    }

    /**
     * Start consuming messages in a background thread.
     */
    public void startAsync() {
        if (messageHandler == null && httpForwarder == null) {
            throw new ConsumerException("No message handler configured. Call subscribe() or forwardTo() first.", "");
        }

        running.set(true);
        executorService.submit(this::consumeLoop);
        logger.info("Consumer started in async mode");
    }

    /**
     * Start consuming messages (blocking).
     */
    public void start() {
        if (messageHandler == null && httpForwarder == null) {
            throw new ConsumerException("No message handler configured. Call subscribe() or forwardTo() first.", "");
        }

        running.set(true);
        consumeLoop();
    }

    private void consumeLoop() {
        try {
            while (running.get()) {
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, Object> record : records) {
                    processRecord(record);
                }

                // Commit offsets manually if auto-commit is disabled
                if (!config.isEnableAutoCommit() && !records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            if (running.get()) {
                logger.error("Error in consumer loop", e);
                errorHandler.handleFatalError(e);
            }
        } finally {
            logger.info("Consumer loop ended");
        }
    }

    private void processRecord(ConsumerRecord<String, Object> record) {
        ConsumedMessage<Object> message = ConsumedMessage.from(record);

        try {
            // Call message handler if configured
            if (messageHandler != null) {
                messageHandler.handle(message);
            }

            // Forward to HTTP endpoint if configured
            if (httpForwarder != null) {
                httpForwarder.forward(message);
            }

            logger.debug("Processed message from topic '{}', partition {}, offset {}",
                record.topic(), record.partition(), record.offset());

        } catch (Exception e) {
            logger.error("Error processing message: {}", e.getMessage());
            errorHandler.handleError(message, e);
        }
    }

    /**
     * Poll for messages once (for manual control).
     *
     * @param timeout Poll timeout
     * @return List of consumed messages
     */
    public List<ConsumedMessage<Object>> poll(Duration timeout) {
        ConsumerRecords<String, Object> records = consumer.poll(timeout);
        List<ConsumedMessage<Object>> messages = new ArrayList<>();

        for (ConsumerRecord<String, Object> record : records) {
            messages.add(ConsumedMessage.from(record));
        }

        return messages;
    }

    /**
     * Commit offsets synchronously.
     */
    public void commitSync() {
        consumer.commitSync();
    }

    /**
     * Commit offsets asynchronously.
     */
    public void commitAsync() {
        consumer.commitAsync((offsets, exception) -> {
            if (exception != null) {
                logger.error("Failed to commit offsets", exception);
            }
        });
    }

    /**
     * Seek to the beginning of all assigned partitions.
     */
    public void seekToBeginning() {
        consumer.seekToBeginning(consumer.assignment());
    }

    /**
     * Seek to the end of all assigned partitions.
     */
    public void seekToEnd() {
        consumer.seekToEnd(consumer.assignment());
    }

    /**
     * Stop consuming messages.
     */
    public void stop() {
        running.set(false);
        logger.info("Consumer stop requested");
    }

    /**
     * Get the schema registry manager for advanced operations.
     */
    public SchemaRegistryManager getSchemaRegistryManager() {
        return schemaRegistryManager;
    }

    @Override
    public void close() {
        logger.info("Closing KafkaConsumer");
        stop();

        try {
            executorService.shutdown();
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        consumer.close(Duration.ofSeconds(30));

        if (httpForwarder != null) {
            try {
                httpForwarder.close();
            } catch (Exception e) {
                logger.warn("Error closing HTTP forwarder", e);
            }
        }
    }

    /**
     * Error handler interface for custom error handling.
     */
    public interface ErrorHandler {
        void handleError(ConsumedMessage<?> message, Exception exception);
        void handleFatalError(Exception exception);
    }

    /**
     * Default error handler that logs errors.
     */
    private static class DefaultErrorHandler implements ErrorHandler {
        @Override
        public void handleError(ConsumedMessage<?> message, Exception exception) {
            logger.error("Error processing message from topic '{}', offset {}: {}",
                message.getTopic(), message.getOffset(), exception.getMessage());
        }

        @Override
        public void handleFatalError(Exception exception) {
            logger.error("Fatal error in consumer", exception);
        }
    }
}

