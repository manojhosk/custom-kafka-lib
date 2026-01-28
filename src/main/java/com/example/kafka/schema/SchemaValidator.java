package com.example.kafka.schema;

import com.example.kafka.exception.SchemaValidationException;

/**
 * Interface for schema validators.
 * Supports different schema types (Avro, JSON Schema).
 */
public interface SchemaValidator {

    /**
     * Validate data against a schema.
     *
     * @param data The data to validate (can be a Map, POJO, or JSON string)
     * @param schema The schema definition
     * @throws SchemaValidationException if validation fails
     */
    void validate(Object data, String schema) throws SchemaValidationException;

    /**
     * Check if data is valid against a schema without throwing an exception.
     *
     * @param data The data to validate
     * @param schema The schema definition
     * @return ValidationResult containing success status and any errors
     */
    ValidationResult isValid(Object data, String schema);

    /**
     * Result of schema validation.
     */
    record ValidationResult(boolean valid, String errors) {
        public static ValidationResult success() {
            return new ValidationResult(true, null);
        }

        public static ValidationResult failure(String errors) {
            return new ValidationResult(false, errors);
        }
    }
}

