package com.example.kafka.schema;

import com.example.kafka.exception.SchemaValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for AvroSchemaValidator.
 */
class AvroSchemaValidatorTest {

    private AvroSchemaValidator validator;

    private static final String USER_SCHEMA = """
        {
          "type": "record",
          "name": "User",
          "namespace": "com.example.kafka.test",
          "fields": [
            {"name": "id", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"}
          ]
        }
        """;

    @BeforeEach
    void setUp() {
        validator = new AvroSchemaValidator();
    }

    @Test
    @DisplayName("Should validate valid data against schema")
    void shouldValidateValidData() {
        Map<String, Object> validUser = Map.of(
                "id", "123",
                "email", "test@example.com",
                "name", "John Doe",
                "age", 30
        );

        assertDoesNotThrow(() -> validator.validate(validUser, USER_SCHEMA));
    }

    @Test
    @DisplayName("Should return success for valid data using isValid")
    void shouldReturnSuccessForValidData() {
        Map<String, Object> validUser = Map.of(
                "id", "123",
                "email", "test@example.com",
                "name", "John Doe",
                "age", 30
        );

        SchemaValidator.ValidationResult result = validator.isValid(validUser, USER_SCHEMA);
        assertTrue(result.valid());
        assertNull(result.errors());
    }

    @Test
    @DisplayName("Should throw exception for missing required field")
    void shouldThrowExceptionForMissingField() {
        Map<String, Object> invalidUser = Map.of(
                "id", "123",
                "email", "test@example.com"
                // missing "name" and "age"
        );

        assertThrows(SchemaValidationException.class,
                () -> validator.validate(invalidUser, USER_SCHEMA));
    }

    @Test
    @DisplayName("Should throw exception for wrong field type")
    void shouldThrowExceptionForWrongType() {
        Map<String, Object> invalidUser = Map.of(
                "id", "123",
                "email", "test@example.com",
                "name", "John Doe",
                "age", "thirty" // should be int
        );

        assertThrows(SchemaValidationException.class,
                () -> validator.validate(invalidUser, USER_SCHEMA));
    }


    @Test
    @DisplayName("Should validate JSON string data")
    void shouldValidateJsonStringData() {
        String jsonData = """
            {
                "id": "123",
                "email": "test@example.com",
                "name": "John Doe",
                "age": 30
            }
            """;

        assertDoesNotThrow(() -> validator.validate(jsonData, USER_SCHEMA));
    }

    @Test
    @DisplayName("Should return failure for invalid schema")
    void shouldReturnFailureForInvalidSchema() {
        Map<String, Object> data = Map.of("id", "123");
        String invalidSchema = "not a valid schema";

        SchemaValidator.ValidationResult result = validator.isValid(data, invalidSchema);
        assertFalse(result.valid());
    }
}

