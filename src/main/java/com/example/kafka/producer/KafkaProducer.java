package com.example.kafka.producer;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.exception.ProducerException;
import com.example.kafka.exception.SchemaValidationException;
import com.example.kafka.registry.SchemaRegistryManager;
import com.example.kafka.schema.SchemaValidator;
import com.example.kafka.schema.SchemaValidatorFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * High-level Kafka producer with schema validation.
 * Provides a simple API for sending schema-validated messages to Kafka.
 */
public class KafkaProducer implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    private final KafkaConfig config;
    private final org.apache.kafka.clients.producer.KafkaProducer<String, Object> producer;
    private final SchemaRegistryManager schemaRegistryManager;
    private final SchemaValidator schemaValidator;
    private final ObjectMapper objectMapper;

    // Cache for schemas per topic
    private final Map<String, String> schemaCache = new HashMap<>();

    /**
     * Create a new KongKafkaProducer with the given configuration.
     *
     * @param config The Kafka configuration
     */
    public KafkaProducer(KafkaConfig config) {
        this.config = config;
        this.schemaRegistryManager = new SchemaRegistryManager(config);
        this.schemaValidator = SchemaValidatorFactory.create(config.getSchemaType());
        this.objectMapper = new ObjectMapper();
        this.producer = createProducer();

        logger.info("KongKafkaProducer initialized with bootstrap servers: {}", config.getBootstrapServers());
    }

    private org.apache.kafka.clients.producer.KafkaProducer<String, Object> createProducer() {
        Properties props = config.toProducerProperties();

        // Key serializer is always String
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Value serializer depends on schema type
        if (config.getSchemaType() == KafkaConfig.SchemaType.AVRO) {
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        } else {
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        }

        return new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    /**
     * Register a schema for a topic.
     * This must be called before sending messages to a topic.
     *
     * @param topic The topic name
     * @param schema The schema definition (Avro or JSON Schema)
     * @return The schema ID
     */
    public int registerSchema(String topic, String schema) {
        String subject = SchemaRegistryManager.valueSubject(topic);
        int schemaId = schemaRegistryManager.registerSchema(subject, schema);
        schemaCache.put(topic, schema);
        logger.info("Registered schema for topic '{}' with ID: {}", topic, schemaId);
        return schemaId;
    }

    /**
     * Send a message to Kafka synchronously.
     * The message is validated against the registered schema before sending.
     *
     * @param topic The topic to send to
     * @param key The message key (can be null)
     * @param value The message value (Map or POJO)
     * @return The send result
     * @throws SchemaValidationException if validation fails
     * @throws ProducerException if sending fails
     */
    public SendResultData send(String topic, String key, Object value) {
        return send(topic, key, value, null);
    }

    /**
     * Send a message to Kafka synchronously with headers.
     *
     * @param topic The topic to send to
     * @param key The message key (can be null)
     * @param value The message value
     * @param headers Optional message headers
     * @return The send result
     */
    public SendResultData send(String topic, String key, Object value, Map<String, String> headers) {
        try {
            return sendAsync(topic, key, value, headers).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Failed to send message to topic '{}': {}", topic, e.getMessage());
            throw new ProducerException("Failed to send message: " + e.getMessage(), topic, e);
        }
    }

    /**
     * Send a message to Kafka asynchronously.
     *
     * @param topic The topic to send to
     * @param key The message key
     * @param value The message value
     * @return A CompletableFuture with the send result
     */
    public CompletableFuture<SendResultData> sendAsync(String topic, String key, Object value) {
        return sendAsync(topic, key, value, null);
    }

    /**
     * Send a message to Kafka asynchronously with headers.
     *
     * @param topic The topic to send to
     * @param key The message key
     * @param value The message value
     * @param headers Optional message headers
     * @return A CompletableFuture with the send result
     */
    public CompletableFuture<SendResultData> sendAsync(String topic, String key, Object value, Map<String, String> headers) {
        CompletableFuture<SendResultData> future = new CompletableFuture<>();

        try {
            // Validate against schema
            String schema = getSchemaForTopic(topic);
            schemaValidator.validate(value, schema);

            // Convert value to appropriate format for serialization
            Object serializedValue = prepareValue(value, schema);

            // Create producer record
            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, serializedValue);

            // Add headers if present
            if (headers != null) {
                headers.forEach((k, v) ->
                    record.headers().add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8))));
            }

            // Send with callback
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to send message to topic '{}': {}", topic, exception.getMessage());
                    future.complete(SendResultData.failure(topic, key, exception));
                } else {
                    logger.debug("Message sent to topic '{}', partition {}, offset {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
                    future.complete(SendResultData.success(
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        metadata.timestamp(),
                        key
                    ));
                }
            });

        } catch (SchemaValidationException e) {
            logger.error("Schema validation failed for topic '{}': {}", topic, e.getValidationErrors());
            future.completeExceptionally(e);
        } catch (Exception e) {
            logger.error("Failed to send message to topic '{}': {}", topic, e.getMessage());
            future.completeExceptionally(new ProducerException("Failed to send message", topic, e));
        }

        return future;
    }

    /**
     * Send multiple messages in a batch.
     *
     * @param topic The topic to send to
     * @param messages List of key-value pairs
     * @return List of send results
     */
    public List<SendResultData> sendBatch(String topic, List<Map.Entry<String, Object>> messages) {
        List<CompletableFuture<SendResultData>> futures = new ArrayList<>();

        for (Map.Entry<String, Object> message : messages) {
            futures.add(sendAsync(topic, message.getKey(), message.getValue()));
        }

        // Wait for all to complete
        return futures.stream()
            .map(f -> {
                try {
                    return f.get(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    return SendResultData.failure(topic, null, e);
                }
            })
            .toList();
    }

    /**
     * Get the schema for a topic, either from cache or from registry.
     */
    private String getSchemaForTopic(String topic) {
        return schemaCache.computeIfAbsent(topic, t -> {
            String subject = SchemaRegistryManager.valueSubject(t);
            if (schemaRegistryManager.subjectExists(subject)) {
                return schemaRegistryManager.getLatestSchema(subject);
            } else {
                throw new ProducerException(
                    "No schema registered for topic: " + t + ". Call registerSchema() first.",
                    t
                );
            }
        });
    }

    /**
     * Prepare the value for serialization based on schema type.
     */
    @SuppressWarnings("unchecked")
    private Object prepareValue(Object value, String schemaString) {
        if (config.getSchemaType() == KafkaConfig.SchemaType.AVRO) {
            // Convert to GenericRecord for Avro
            if (value instanceof GenericRecord) {
                return value;
            }

            Schema schema = new Schema.Parser().parse(schemaString);
            GenericRecord record = new GenericData.Record(schema);

            Map<String, Object> data;
            if (value instanceof Map) {
                data = (Map<String, Object>) value;
            } else {
                // Convert POJO to Map
                data = objectMapper.convertValue(value, Map.class);
            }

            for (Schema.Field field : schema.getFields()) {
                Object fieldValue = data.get(field.name());
                if (fieldValue != null) {
                    record.put(field.name(), fieldValue);
                }
            }

            return record;
        } else {
            // JSON Schema - return as-is, the serializer will handle it
            return value;
        }
    }

    /**
     * Flush any buffered messages.
     */
    public void flush() {
        producer.flush();
    }

    /**
     * Get the schema registry manager for advanced operations.
     */
    public SchemaRegistryManager getSchemaRegistryManager() {
        return schemaRegistryManager;
    }

    @Override
    public void close() {
        logger.info("Closing KafkaProducer");
        producer.close(Duration.ofSeconds(30));
    }
}

