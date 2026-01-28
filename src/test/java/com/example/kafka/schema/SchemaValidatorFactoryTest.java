package com.example.kafka.schema;

import com.example.kafka.config.KafkaConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SchemaValidatorFactory.
 */
class SchemaValidatorFactoryTest {

    @Test
    @DisplayName("Should create AvroSchemaValidator for AVRO type")
    void shouldCreateAvroValidator() {
        SchemaValidator validator = SchemaValidatorFactory.create(KafkaConfig.SchemaType.AVRO);

        assertNotNull(validator);
        assertInstanceOf(AvroSchemaValidator.class, validator);
    }

    @Test
    @DisplayName("Should create JsonSchemaValidator for JSON_SCHEMA type")
    void shouldCreateJsonSchemaValidator() {
        SchemaValidator validator = SchemaValidatorFactory.create(KafkaConfig.SchemaType.JSON_SCHEMA);

        assertNotNull(validator);
        assertInstanceOf(JsonSchemaValidator.class, validator);
    }

    @Test
    @DisplayName("Should throw exception for null schema type")
    void shouldThrowExceptionForNullType() {
        assertThrows(NullPointerException.class,
                () -> SchemaValidatorFactory.create(null));
    }
}

