package com.example;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.consumer.ConsumedMessage;
import com.example.kafka.consumer.HttpForwarder;
import com.example.kafka.consumer.KafkaConsumer;
import com.example.kafka.producer.KafkaProducer;
import com.example.kafka.producer.SendResultData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Example application demonstrating Custom Kafka Library usage.
 *
 * This example shows:
 * 1. How to configure the library
 * 2. How to register a schema
 * 3. How to produce messages
 * 4. How to consume messages
 * 5. How to forward messages to an HTTP endpoint
 */
public class ExampleApp {

    private static final Logger logger = LoggerFactory.getLogger(ExampleApp.class);

    private static final String TOPIC = "users";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String FORWARD_URL = "http://localhost:1080/api/users";

    // User schema (Avro)
    private static final String USER_SCHEMA = """
        {
          "type": "record",
          "name": "User",
          "namespace": "com.example.kafka.examples",
          "fields": [
            {"name": "id", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "createdAt", "type": "long"}
          ]
        }
        """;

    public static void main(String[] args) throws Exception {
        logger.info("Starting Custom Kafka Library Example Application");

        // Determine which mode to run
        String mode = args.length > 0 ? args[0] : "all";

        switch (mode) {
            case "producer" -> runProducer();
            case "consumer" -> runConsumer();
            case "all" -> runAll();
            default -> {
                logger.error("Unknown mode: {}. Use 'producer', 'consumer', or 'all'", mode);
                System.exit(1);
            }
        }
    }

    /**
     * Run only the producer.
     */
    private static void runProducer() {
        logger.info("Running in producer mode");

        // Create configuration
        KafkaConfig config = KafkaConfig.builder()
            .bootstrapServers(BOOTSTRAP_SERVERS)
            .schemaRegistryUrl(SCHEMA_REGISTRY_URL)
            .schemaType(KafkaConfig.SchemaType.AVRO)
            .acks("all")                    // Wait for all replicas
            .enableIdempotence(true)        // Ensure exactly-once semantics
            .retries(3)                     // Retry on failure
            .build();

        try (KafkaProducer producer = new KafkaProducer(config)) {
            // Register schema (only needed once, but idempotent)
            producer.registerSchema(TOPIC, USER_SCHEMA);
            logger.info("Schema registered for topic: {}", TOPIC);

            // Send some sample users
            for (int i = 1; i <= 5; i++) {
                Map<String, Object> user = createUser(i);
                String key = (String) user.get("id");

                SendResultData result = producer.send(TOPIC, key, user);

                if (result.isSuccess()) {
                    logger.info("✓ Sent user {}: partition={}, offset={}",
                        key, result.getPartition(), result.getOffset());
                } else {
                    logger.error("✗ Failed to send user {}: {}",
                        key, result.getError().getMessage());
                }
            }

            producer.flush();
            logger.info("All messages sent successfully!");

        } catch (Exception e) {
            logger.error("Producer error", e);
        }
    }

    /**
     * Run only the consumer.
     */
    private static void runConsumer() throws InterruptedException {
        logger.info("Running in consumer mode");

        // Create configuration
        KafkaConfig config = KafkaConfig.builder()
            .bootstrapServers(BOOTSTRAP_SERVERS)
            .schemaRegistryUrl(SCHEMA_REGISTRY_URL)
            .schemaType(KafkaConfig.SchemaType.AVRO)
            .groupId("example-consumer-group-1")
            .autoOffsetReset("earliest")    // Start from beginning
            .enableAutoCommit(false)        // Manual commit for reliability
            .build();

        // Create HTTP forwarder (optional - forwards consumed messages to an API)
        HttpForwarder forwarder = HttpForwarder.builder(FORWARD_URL)
            .maxRetries(3)
            .retryDelayMs(1000)
            .header("X-Source", "custom-kafka-consumer")
            .build();

        CountDownLatch latch = new CountDownLatch(1);

        // Handle shutdown gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown requested");
            latch.countDown();
        }));

        try (KafkaConsumer consumer = new KafkaConsumer(config)) {
            // Subscribe to topic with a message handler
            consumer.subscribe(TOPIC, message -> {
                logger.info("✓ Received message: topic={}, partition={}, offset={}, key={}",
                    message.getTopic(),
                    message.getPartition(),
                    message.getOffset(),
                    message.getKey());
                logger.info("  Value: {}", message.getValue());
            });

            // Optionally forward to HTTP endpoint
            consumer.forwardTo(forwarder);

            // Set custom error handler
            consumer.withErrorHandler(new KafkaConsumer.ErrorHandler() {
                @Override
                public void handleError(ConsumedMessage<?> message, Exception exception) {
                    logger.error("Error processing message at offset {}: {}",
                        message.getOffset(), exception.getMessage());
                    // In production, you might send to a dead letter queue here
                }

                @Override
                public void handleFatalError(Exception exception) {
                    logger.error("Fatal consumer error", exception);
                    latch.countDown();
                }
            });

            // Start consuming in background
            consumer.startAsync();
            logger.info("Consumer started. Press Ctrl+C to stop.");

            // Wait for shutdown signal
            latch.await();

        } catch (Exception e) {
            logger.error("Consumer error", e);
        }
    }

    /**
     * Run both producer and consumer.
     */
    private static void runAll() throws Exception {
        logger.info("Running producer and consumer together");

        // Start consumer in a separate thread
        Thread consumerThread = new Thread(() -> {
            try {
                runConsumer();
            } catch (Exception e) {
                logger.error("Consumer thread error", e);
            }
        });
        consumerThread.start();

        // Give consumer time to start
        Thread.sleep(3000);

        // Run producer
        runProducer();

        // Wait a bit for consumer to process messages
        Thread.sleep(5000);

        logger.info("Example completed. Press Ctrl+C to exit.");
    }

    /**
     * Create a sample user map.
     */
    private static Map<String, Object> createUser(int index) {
        Map<String, Object> user = new HashMap<>();
        user.put("id", UUID.randomUUID().toString());
        user.put("email", "user" + index + "@example.com");
        user.put("name", "User " + index);
        user.put("createdAt", System.currentTimeMillis());
        return user;
    }
}

