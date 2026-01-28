// Package schema provides schema validation for Avro and JSON schemas.
package schema

import (
	"github.com/example/custom-kafka-lib/errors"
)

// ValidationResult holds the result of a schema validation.
type ValidationResult struct {
	Valid  bool
	Errors string
}

// Success returns a successful validation result.
func Success() ValidationResult {
	return ValidationResult{Valid: true}
}

// Failure returns a failed validation result.
func Failure(errs string) ValidationResult {
	return ValidationResult{Valid: false, Errors: errs}
}

// Validator defines the interface for schema validators.
type Validator interface {
	// Validate validates data against a schema and returns an error if validation fails.
	Validate(data interface{}, schema string) error

	// IsValid validates data against a schema and returns a ValidationResult.
	IsValid(data interface{}, schema string) ValidationResult
}

// ValidatorFactory creates the appropriate validator based on schema type.
type ValidatorFactory struct{}

// NewValidatorFactory creates a new ValidatorFactory.
func NewValidatorFactory() *ValidatorFactory {
	return &ValidatorFactory{}
}

// Create creates a validator for the given schema type.
func (f *ValidatorFactory) Create(schemaType string) Validator {
	switch schemaType {
	case "AVRO":
		return NewAvroValidator()
	case "JSON":
		return NewJSONValidator()
	default:
		return NewAvroValidator() // Default to Avro
	}
}

// ValidateOrError is a helper function that validates and returns a SchemaValidationError if validation fails.
func ValidateOrError(validator Validator, data interface{}, schema string) error {
	result := validator.IsValid(data, schema)
	if !result.Valid {
		return errors.NewSchemaValidationError("Schema validation failed", schema, result.Errors)
	}
	return nil
}

