package schema

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/example/custom-kafka-lib/errors"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// JSONValidator validates data against JSON schemas.
type JSONValidator struct{}

// NewJSONValidator creates a new JSONValidator.
func NewJSONValidator() *JSONValidator {
	return &JSONValidator{}
}

// Validate validates data against a JSON schema.
func (v *JSONValidator) Validate(data interface{}, schema string) error {
	result := v.IsValid(data, schema)
	if !result.Valid {
		return errors.NewSchemaValidationError("JSON schema validation failed", schema, result.Errors)
	}
	return nil
}

// IsValid validates data against a JSON schema and returns a ValidationResult.
func (v *JSONValidator) IsValid(data interface{}, schemaStr string) ValidationResult {
	// Compile the JSON schema
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("schema.json", strings.NewReader(schemaStr)); err != nil {
		return Failure(fmt.Sprintf("invalid JSON schema: %v", err))
	}

	schema, err := compiler.Compile("schema.json")
	if err != nil {
		return Failure(fmt.Sprintf("failed to compile JSON schema: %v", err))
	}

	// Convert data to interface{} if needed
	var jsonData interface{}
	switch d := data.(type) {
	case string:
		if err := json.Unmarshal([]byte(d), &jsonData); err != nil {
			return Failure(fmt.Sprintf("failed to parse JSON data: %v", err))
		}
	case []byte:
		if err := json.Unmarshal(d, &jsonData); err != nil {
			return Failure(fmt.Sprintf("failed to parse JSON data: %v", err))
		}
	case map[string]interface{}:
		jsonData = d
	default:
		// Try to convert to map via JSON
		jsonBytes, err := json.Marshal(data)
		if err != nil {
			return Failure(fmt.Sprintf("failed to convert data to JSON: %v", err))
		}
		if err := json.Unmarshal(jsonBytes, &jsonData); err != nil {
			return Failure(fmt.Sprintf("failed to parse JSON data: %v", err))
		}
	}

	// Validate the data
	if err := schema.Validate(jsonData); err != nil {
		return Failure(fmt.Sprintf("JSON schema validation error: %v", err))
	}

	return Success()
}

