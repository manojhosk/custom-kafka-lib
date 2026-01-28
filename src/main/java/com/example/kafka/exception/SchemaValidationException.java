package com.example.kafka.exception;

/**
 * Exception thrown when schema validation fails.
 */
public class SchemaValidationException extends CustomKafkaException {

    private final String schemaId;
    private final String validationErrors;

    public SchemaValidationException(String message, String schemaId, String validationErrors) {
        super(message);
        this.schemaId = schemaId;
        this.validationErrors = validationErrors;
    }

    public SchemaValidationException(String message, String schemaId, String validationErrors, Throwable cause) {
        super(message, cause);
        this.schemaId = schemaId;
        this.validationErrors = validationErrors;
    }

    public String getSchemaId() {
        return schemaId;
    }

    public String getValidationErrors() {
        return validationErrors;
    }

    @Override
    public String toString() {
        return String.format("SchemaValidationException{schemaId='%s', errors='%s', message='%s'}",
            schemaId, validationErrors, getMessage());
    }
}

