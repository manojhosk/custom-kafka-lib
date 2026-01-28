package com.example.kafka.schema;

import com.example.kafka.config.KafkaConfig;

/**
 * Factory for creating schema validators based on configuration.
 */
public class SchemaValidatorFactory {

    /**
     * Create a schema validator based on the schema type.
     *
     * @param schemaType The type of schema (AVRO or JSON_SCHEMA)
     * @return The appropriate SchemaValidator implementation
     */
    public static SchemaValidator create(KafkaConfig.SchemaType schemaType) {
        return switch (schemaType) {
            case AVRO -> new AvroSchemaValidator();
            case JSON_SCHEMA -> new JsonSchemaValidator();
        };
    }
}

