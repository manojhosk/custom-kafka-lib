package schema

import (
	"encoding/json"
	"fmt"

	"github.com/example/custom-kafka-lib/errors"
	"github.com/linkedin/goavro/v2"
)

// AvroValidator validates data against Avro schemas.
type AvroValidator struct{}

// NewAvroValidator creates a new AvroValidator.
func NewAvroValidator() *AvroValidator {
	return &AvroValidator{}
}

// Validate validates data against an Avro schema.
func (v *AvroValidator) Validate(data interface{}, schema string) error {
	result := v.IsValid(data, schema)
	if !result.Valid {
		return errors.NewSchemaValidationError("Avro schema validation failed", schema, result.Errors)
	}
	return nil
}

// IsValid validates data against an Avro schema and returns a ValidationResult.
func (v *AvroValidator) IsValid(data interface{}, schema string) ValidationResult {
	// Parse the Avro schema
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return Failure(fmt.Sprintf("invalid Avro schema: %v", err))
	}

	// Convert data to native Go type if needed
	var nativeData interface{}
	switch d := data.(type) {
	case string:
		// Try to parse as JSON
		if err := json.Unmarshal([]byte(d), &nativeData); err != nil {
			return Failure(fmt.Sprintf("failed to parse JSON data: %v", err))
		}
	case []byte:
		// Try to parse as JSON
		if err := json.Unmarshal(d, &nativeData); err != nil {
			return Failure(fmt.Sprintf("failed to parse JSON data: %v", err))
		}
	case map[string]interface{}:
		nativeData = d
	default:
		// Try to convert to map via JSON
		jsonBytes, err := json.Marshal(data)
		if err != nil {
			return Failure(fmt.Sprintf("failed to convert data to JSON: %v", err))
		}
		if err := json.Unmarshal(jsonBytes, &nativeData); err != nil {
			return Failure(fmt.Sprintf("failed to parse JSON data: %v", err))
		}
	}

	// Try to encode the data using the schema (this validates it)
	_, err = codec.BinaryFromNative(nil, nativeData)
	if err != nil {
		return Failure(fmt.Sprintf("Avro validation error: %v", err))
	}

	return Success()
}

// EncodeAvro encodes data to Avro binary format.
func (v *AvroValidator) EncodeAvro(data interface{}, schema string) ([]byte, error) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("invalid Avro schema: %w", err)
	}

	// Convert data to native Go type if needed
	var nativeData interface{}
	switch d := data.(type) {
	case map[string]interface{}:
		nativeData = d
	default:
		jsonBytes, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("failed to convert data to JSON: %w", err)
		}
		if err := json.Unmarshal(jsonBytes, &nativeData); err != nil {
			return nil, fmt.Errorf("failed to parse JSON data: %w", err)
		}
	}

	return codec.BinaryFromNative(nil, nativeData)
}

// DecodeAvro decodes Avro binary data.
func (v *AvroValidator) DecodeAvro(data []byte, schema string) (interface{}, error) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("invalid Avro schema: %w", err)
	}

	native, _, err := codec.NativeFromBinary(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Avro data: %w", err)
	}

	return native, nil
}

