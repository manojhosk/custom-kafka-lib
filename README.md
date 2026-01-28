# Custom Kafka Library

A standardized Java library for producing and consuming Kafka messages with schema validation, designed to provide a seamless developer experience for teams without deep Kafka expertise.

## Problem Statement

Many application teams want to send and consume data from Kafka but have varying approaches, leading to fragmentation and inconsistent data quality. This library provides:

- **Standardization** in how applications produce/consume data (serialization, schema registry)
- **Seamless Developer Experience** for quick onboarding without Kafka expertise
- **Schema Enforcement** ensuring only valid messages are produced

## Getting Started

### Clone and Build

```bash
# Clone the repository
git clone <git_url>
cd custom-kafka-lib

# Build and install to local Maven repository
mvn clean install

# (Optional) Skip tests for faster build
mvn clean install -DskipTests
```

### Start Infrastructure

```bash
cd docker && docker-compose up -d
```

This starts Kafka (port 9092) and Schema Registry (port 8081).

### Add Dependency to Your Project

```xml
<dependency>
    <groupId>com.example</groupId>
    <artifactId>custom-kafka-lib</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Example Usage

### Define a Schema

```java
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
```

### Produce Data to Kafka

```java
KafkaConfig config = KafkaConfig.builder()
    .bootstrapServers("localhost:9092")
    .schemaRegistryUrl("http://localhost:8081")
    .schemaType(KafkaConfig.SchemaType.AVRO)
    .build();

try (KafkaProducer producer = new KafkaProducer(config)) {
    // Register schema (enforces validation)
    producer.registerSchema("users", USER_SCHEMA);
    
    // Send message - automatically validated against schema
    Map<String, Object> user = Map.of(
        "id", "123",
        "email", "user@example.com",
        "name", "John Doe",
        "createdAt", System.currentTimeMillis()
    );
    
    SendResultData result = producer.send("users", "123", user);
}
```

### Consume Data and Forward to API

```java
KafkaConfig config = KafkaConfig.builder()
    .bootstrapServers("localhost:9092")
    .schemaRegistryUrl("http://localhost:8081")
    .groupId("my-consumer-group")
    .build();

// Configure HTTP forwarding (e.g., to Elasticsearch)
HttpForwarder forwarder = HttpForwarder.builder("http://localhost:9200/users/_doc")
    .maxRetries(3)
    .build();

try (KafkaConsumer consumer = new KafkaConsumer(config)) {
    consumer.subscribe("users", message -> {
        System.out.println("Received: " + message.getValue());
    });
    consumer.forwardTo(forwarder);
    consumer.startAsync();
}
```

## API Design Philosophy

### Why This Abstraction?

1. **Hide Complexity**: Teams don't need Kafka expertise - just define schema and send/receive
2. **Enforce Standards**: Schema validation is mandatory, ensuring data quality
3. **Consistent Patterns**: Same API across all applications reduces fragmentation
4. **Pluggable**: Supports both Avro and JSON Schema; extensible error handling

### Core Components

| Component | Purpose |
|-----------|---------|
| `KafkaConfig` | Fluent configuration builder |
| `KafkaProducer` | Schema-validated message production |
| `KafkaConsumer` | Message consumption with handlers |
| `HttpForwarder` | Forward messages to REST APIs (e.g., Elasticsearch) |
| `SchemaRegistryManager` | Schema registration and versioning |

## Schema Versioning Strategy

### Approach: Backward Compatibility

Schemas are registered and versioned in the Schema Registry. The library enforces that messages conform to the registered schema.

```
Schema v1 → Library v1.0.x
Schema v2 → Library v1.1.x (backward compatible)
```

**Evolution Rules:**
- ✓ Add optional field with default
- ✓ Remove optional field  
- ✗ Remove required field
- ✗ Change field type

### Registering Schemas

```java
// Schema is registered automatically when calling registerSchema()
producer.registerSchema("users", USER_SCHEMA);

// Or access registry directly for version management
SchemaRegistryManager registry = producer.getSchemaRegistryManager();
int schemaId = registry.registerSchema("users-value", schemaString);
```

## Failure Handling

| Scenario | Handling |
|----------|----------|
| **Schema Validation Fails** | `SchemaValidationException` thrown immediately - invalid data never sent |
| **Kafka Unavailable** | Automatic retry with exponential backoff (configurable) |
| **HTTP Forward Fails** | Retry with exponential backoff; logged after max attempts |
| **Consumer Error** | Custom `ErrorHandler` interface for DLQ, alerting, etc. |

### Reliability Features

- **Idempotent Producer**: Enabled by default (`enableIdempotence=true`) for exactly-once semantics
- **Acknowledgments**: `acks=all` ensures messages are replicated before acknowledgment
- **Manual Commits**: Auto-commit disabled by default for precise offset control

## Project Structure

```
custom-kafka-lib/
├── src/main/java/com/example/kafka/
│   ├── config/          # KafkaConfig
│   ├── producer/        # KafkaProducer
│   ├── consumer/        # KafkaConsumer, HttpForwarder
│   ├── schema/          # Schema validators
│   ├── registry/        # SchemaRegistryManager
│   └── exception/       # Custom exceptions
├── docker/              # Kafka + Schema Registry
├── schemas/             # Example schemas
└── example-app/         # Demo application
```

## Running the Example App

```bash
# Terminal 1: Start infrastructure
cd docker && docker-compose up -d

# Terminal 2: Build and run
mvn clean install
cd example-app
mvn exec:java -Dexec.mainClass="com.example.ExampleApp"
```

## Requirements

- Java 17+
- Maven 3.6+
- Docker (for local Kafka/Schema Registry)

## Running Tests

```bash
# Run all tests
mvn test

# Run tests with code coverage report
mvn clean test

# View coverage report (after running tests)
open target/site/jacoco/index.html
```

The coverage report will be generated at `target/site/jacoco/index.html`.

