package com.example;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.producer.KafkaProducer;
import com.example.kafka.producer.SendResultData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Example demonstrating JSON Schema usage with the Custom Kafka Library.
 */
public class JsonSchemaExample {

    private static final Logger logger = LoggerFactory.getLogger(JsonSchemaExample.class);

    private static final String TOPIC = "orders";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    // Order schema (JSON Schema)
    private static final String ORDER_SCHEMA = """
        {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "title": "Order",
          "type": "object",
          "required": ["orderId", "customerId", "items", "totalAmount", "createdAt"],
          "properties": {
            "orderId": {"type": "string"},
            "customerId": {"type": "string"},
            "items": {
              "type": "array",
              "items": {
                "type": "object",
                "required": ["productId", "quantity", "price"],
                "properties": {
                  "productId": {"type": "string"},
                  "quantity": {"type": "integer", "minimum": 1},
                  "price": {"type": "number", "minimum": 0}
                }
              }
            },
            "totalAmount": {"type": "number", "minimum": 0},
            "createdAt": {"type": "integer"}
          }
        }
        """;

    public static void main(String[] args) {
        logger.info("Starting JSON Schema Example");

        // Create configuration for JSON Schema
        KafkaConfig config = KafkaConfig.builder()
            .bootstrapServers(BOOTSTRAP_SERVERS)
            .schemaRegistryUrl(SCHEMA_REGISTRY_URL)
            .schemaType(KafkaConfig.SchemaType.JSON_SCHEMA)  // Use JSON Schema
            .build();

        try (KafkaProducer producer = new KafkaProducer(config)) {
            // Register schema
            producer.registerSchema(TOPIC, ORDER_SCHEMA);
            logger.info("JSON Schema registered for topic: {}", TOPIC);

            // Create and send a valid order
            Map<String, Object> order = createOrder();
            SendResultData result = producer.send(TOPIC, (String) order.get("orderId"), order);

            if (result.isSuccess()) {
                logger.info("✓ Order sent successfully: partition={}, offset={}",
                    result.getPartition(), result.getOffset());
            }

            // Try to send an invalid order (missing required field)
            try {
                Map<String, Object> invalidOrder = new HashMap<>();
                invalidOrder.put("orderId", UUID.randomUUID().toString());
                // Missing required fields: customerId, items, totalAmount, createdAt

                producer.send(TOPIC, (String) invalidOrder.get("orderId"), invalidOrder);
                logger.error("Should have thrown validation exception!");
            } catch (Exception e) {
                logger.info("✓ Correctly rejected invalid order: {}", e.getMessage());
            }

            producer.flush();

        } catch (Exception e) {
            logger.error("Error", e);
        }
    }

    private static Map<String, Object> createOrder() {
        Map<String, Object> order = new HashMap<>();
        order.put("orderId", UUID.randomUUID().toString());
        order.put("customerId", "CUST-123");
        order.put("items", List.of(
            Map.of("productId", "PROD-001", "quantity", 2, "price", 29.99),
            Map.of("productId", "PROD-002", "quantity", 1, "price", 49.99)
        ));
        order.put("totalAmount", 109.97);
        order.put("createdAt", System.currentTimeMillis());
        return order;
    }
}

