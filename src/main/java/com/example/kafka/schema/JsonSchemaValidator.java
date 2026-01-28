package com.example.kafka.schema;

import com.example.kafka.exception.SchemaValidationException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * JSON Schema validator implementation.
 * Uses networknt/json-schema-validator for validation.
 */
public class JsonSchemaValidator implements SchemaValidator {

    private static final Logger logger = LoggerFactory.getLogger(JsonSchemaValidator.class);
    private final ObjectMapper objectMapper;
    private final JsonSchemaFactory schemaFactory;

    public JsonSchemaValidator() {
        this.objectMapper = new ObjectMapper();
        this.schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
    }

    @Override
    public void validate(Object data, String schema) throws SchemaValidationException {
        ValidationResult result = isValid(data, schema);
        if (!result.valid()) {
            throw new SchemaValidationException(
                "JSON Schema validation failed",
                schema,
                result.errors()
            );
        }
    }

    @Override
    public ValidationResult isValid(Object data, String schema) {
        try {
            JsonNode schemaNode = objectMapper.readTree(schema);
            JsonSchema jsonSchema = schemaFactory.getSchema(schemaNode);

            JsonNode dataNode;
            if (data instanceof String) {
                dataNode = objectMapper.readTree((String) data);
            } else if (data instanceof JsonNode) {
                dataNode = (JsonNode) data;
            } else {
                dataNode = objectMapper.valueToTree(data);
            }

            Set<ValidationMessage> errors = jsonSchema.validate(dataNode);

            if (errors.isEmpty()) {
                logger.debug("JSON Schema validation passed");
                return ValidationResult.success();
            } else {
                String errorMessage = errors.stream()
                    .map(ValidationMessage::getMessage)
                    .collect(Collectors.joining("; "));
                logger.warn("JSON Schema validation failed: {}", errorMessage);
                return ValidationResult.failure(errorMessage);
            }
        } catch (Exception e) {
            logger.error("Error during JSON Schema validation", e);
            return ValidationResult.failure("Validation error: " + e.getMessage());
        }
    }
}

