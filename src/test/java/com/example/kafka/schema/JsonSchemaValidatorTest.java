package com.example.kafka.schema;

import com.example.kafka.exception.SchemaValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for JsonSchemaValidator.
 */
class JsonSchemaValidatorTest {

    private JsonSchemaValidator validator;

    private static final String ORDER_SCHEMA = """
        {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "type": "object",
          "required": ["orderId", "customerId", "items", "totalAmount"],
          "properties": {
            "orderId": {
              "type": "string"
            },
            "customerId": {
              "type": "string"
            },
            "items": {
              "type": "array",
              "items": {
                "type": "object",
                "required": ["productId", "quantity"],
                "properties": {
                  "productId": {"type": "string"},
                  "quantity": {"type": "integer", "minimum": 1}
                }
              }
            },
            "totalAmount": {
              "type": "number",
              "minimum": 0
            },
            "notes": {
              "type": "string"
            }
          }
        }
        """;

    @BeforeEach
    void setUp() {
        validator = new JsonSchemaValidator();
    }

    @Test
    @DisplayName("Should validate valid data against schema")
    void shouldValidateValidData() {
        Map<String, Object> validOrder = Map.of(
                "orderId", "ORD-001",
                "customerId", "CUST-123",
                "items", List.of(
                        Map.of("productId", "PROD-1", "quantity", 2),
                        Map.of("productId", "PROD-2", "quantity", 1)
                ),
                "totalAmount", 99.99
        );

        assertDoesNotThrow(() -> validator.validate(validOrder, ORDER_SCHEMA));
    }

    @Test
    @DisplayName("Should return success for valid data using isValid")
    void shouldReturnSuccessForValidData() {
        Map<String, Object> validOrder = Map.of(
                "orderId", "ORD-001",
                "customerId", "CUST-123",
                "items", List.of(Map.of("productId", "PROD-1", "quantity", 2)),
                "totalAmount", 50.0
        );

        SchemaValidator.ValidationResult result = validator.isValid(validOrder, ORDER_SCHEMA);
        assertTrue(result.valid());
        assertNull(result.errors());
    }

    @Test
    @DisplayName("Should throw exception for missing required field")
    void shouldThrowExceptionForMissingField() {
        Map<String, Object> invalidOrder = Map.of(
                "orderId", "ORD-001",
                "customerId", "CUST-123"
                // missing "items" and "totalAmount"
        );

        assertThrows(SchemaValidationException.class,
                () -> validator.validate(invalidOrder, ORDER_SCHEMA));
    }

    @Test
    @DisplayName("Should throw exception for wrong field type")
    void shouldThrowExceptionForWrongType() {
        Map<String, Object> invalidOrder = Map.of(
                "orderId", "ORD-001",
                "customerId", "CUST-123",
                "items", List.of(Map.of("productId", "PROD-1", "quantity", 2)),
                "totalAmount", "not a number" // should be number
        );

        assertThrows(SchemaValidationException.class,
                () -> validator.validate(invalidOrder, ORDER_SCHEMA));
    }

    @Test
    @DisplayName("Should throw exception for negative totalAmount")
    void shouldThrowExceptionForNegativeAmount() {
        Map<String, Object> invalidOrder = Map.of(
                "orderId", "ORD-001",
                "customerId", "CUST-123",
                "items", List.of(Map.of("productId", "PROD-1", "quantity", 2)),
                "totalAmount", -10.0 // minimum is 0
        );

        assertThrows(SchemaValidationException.class,
                () -> validator.validate(invalidOrder, ORDER_SCHEMA));
    }

    @Test
    @DisplayName("Should throw exception for zero quantity")
    void shouldThrowExceptionForZeroQuantity() {
        Map<String, Object> invalidOrder = Map.of(
                "orderId", "ORD-001",
                "customerId", "CUST-123",
                "items", List.of(Map.of("productId", "PROD-1", "quantity", 0)), // minimum is 1
                "totalAmount", 50.0
        );

        assertThrows(SchemaValidationException.class,
                () -> validator.validate(invalidOrder, ORDER_SCHEMA));
    }

    @Test
    @DisplayName("Should validate data with optional field")
    void shouldValidateDataWithOptionalField() {
        Map<String, Object> orderWithNotes = Map.of(
                "orderId", "ORD-001",
                "customerId", "CUST-123",
                "items", List.of(Map.of("productId", "PROD-1", "quantity", 1)),
                "totalAmount", 25.0,
                "notes", "Please deliver after 5pm"
        );

        assertDoesNotThrow(() -> validator.validate(orderWithNotes, ORDER_SCHEMA));
    }

    @Test
    @DisplayName("Should validate JSON string data")
    void shouldValidateJsonStringData() {
        String jsonData = """
            {
                "orderId": "ORD-001",
                "customerId": "CUST-123",
                "items": [{"productId": "PROD-1", "quantity": 2}],
                "totalAmount": 50.0
            }
            """;

        assertDoesNotThrow(() -> validator.validate(jsonData, ORDER_SCHEMA));
    }

    @Test
    @DisplayName("Should return failure for invalid schema")
    void shouldReturnFailureForInvalidSchema() {
        Map<String, Object> data = Map.of("id", "123");
        String invalidSchema = "not a valid JSON schema";

        SchemaValidator.ValidationResult result = validator.isValid(data, invalidSchema);
        assertFalse(result.valid());
    }

    @Test
    @DisplayName("Should return errors in validation result")
    void shouldReturnErrorsInValidationResult() {
        Map<String, Object> invalidOrder = Map.of(
                "orderId", "ORD-001"
                // missing required fields
        );

        SchemaValidator.ValidationResult result = validator.isValid(invalidOrder, ORDER_SCHEMA);
        assertFalse(result.valid());
        assertNotNull(result.errors());
        assertFalse(result.errors().isEmpty());
    }
}

