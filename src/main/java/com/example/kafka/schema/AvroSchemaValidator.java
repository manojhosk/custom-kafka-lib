package com.example.kafka.schema;

import com.example.kafka.exception.SchemaValidationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Avro Schema validator implementation.
 * Validates data against Avro schemas.
 */
public class AvroSchemaValidator implements SchemaValidator {

    private static final Logger logger = LoggerFactory.getLogger(AvroSchemaValidator.class);
    private final ObjectMapper objectMapper;

    public AvroSchemaValidator() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void validate(Object data, String schemaString) throws SchemaValidationException {
        ValidationResult result = isValid(data, schemaString);
        if (!result.valid()) {
            throw new SchemaValidationException(
                "Avro Schema validation failed",
                schemaString,
                result.errors()
            );
        }
    }

    @Override
    public ValidationResult isValid(Object data, String schemaString) {
        try {
            Schema schema = new Schema.Parser().parse(schemaString);

            // Convert data to JSON string if it's not already
            String jsonData;
            if (data instanceof String) {
                jsonData = (String) data;
            } else if (data instanceof GenericRecord) {
                // Already a GenericRecord, just validate
                return validateGenericRecord((GenericRecord) data, schema);
            } else {
                jsonData = objectMapper.writeValueAsString(data);
            }

            // Try to parse the JSON data against the Avro schema
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonData);
            reader.read(null, decoder);

            logger.debug("Avro Schema validation passed");
            return ValidationResult.success();
        } catch (Exception e) {
            logger.warn("Avro Schema validation failed: {}", e.getMessage());
            return ValidationResult.failure("Avro validation error: " + e.getMessage());
        }
    }

    private ValidationResult validateGenericRecord(GenericRecord record, Schema schema) {
        try {
            // Validate that the record conforms to the schema
            if (!record.getSchema().equals(schema)) {
                return ValidationResult.failure("Record schema does not match expected schema");
            }

            // Validate each field
            for (Schema.Field field : schema.getFields()) {
                Object value = record.get(field.name());
                if (value == null && field.schema().getType() != Schema.Type.NULL) {
                    // Check if it's a union with null
                    boolean allowsNull = field.schema().getType() == Schema.Type.UNION &&
                        field.schema().getTypes().stream()
                            .anyMatch(t -> t.getType() == Schema.Type.NULL);
                    if (!allowsNull && field.defaultVal() == null) {
                        return ValidationResult.failure("Required field '" + field.name() + "' is null");
                    }
                }
            }

            return ValidationResult.success();
        } catch (Exception e) {
            return ValidationResult.failure("Validation error: " + e.getMessage());
        }
    }

    /**
     * Create a GenericRecord from a Map using the given schema.
     *
     * @param data The data as a Map
     * @param schemaString The Avro schema
     * @return A GenericRecord
     */
    public GenericRecord createGenericRecord(Map<String, Object> data, String schemaString) {
        Schema schema = new Schema.Parser().parse(schemaString);
        GenericRecord record = new GenericData.Record(schema);

        for (Schema.Field field : schema.getFields()) {
            Object value = data.get(field.name());
            if (value != null) {
                record.put(field.name(), value);
            }
        }

        return record;
    }
}

