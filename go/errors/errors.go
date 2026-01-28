// Package errors provides custom error types for the Custom Kafka library.
package errors

import "fmt"

// KafkaError is the base error type for all Custom Kafka library errors.
type KafkaError struct {
	Message string
	Cause   error
}

func (e *KafkaError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *KafkaError) Unwrap() error {
	return e.Cause
}

// ProducerError represents errors that occur during message production.
type ProducerError struct {
	KafkaError
	Topic string
}

func NewProducerError(message, topic string, cause error) *ProducerError {
	return &ProducerError{
		KafkaError: KafkaError{Message: message, Cause: cause},
		Topic:      topic,
	}
}

func (e *ProducerError) Error() string {
	return fmt.Sprintf("producer error on topic '%s': %s", e.Topic, e.KafkaError.Error())
}

// ConsumerError represents errors that occur during message consumption.
type ConsumerError struct {
	KafkaError
	Topic string
}

func NewConsumerError(message, topic string, cause error) *ConsumerError {
	return &ConsumerError{
		KafkaError: KafkaError{Message: message, Cause: cause},
		Topic:      topic,
	}
}

func (e *ConsumerError) Error() string {
	return fmt.Sprintf("consumer error on topic '%s': %s", e.Topic, e.KafkaError.Error())
}

// SchemaValidationError represents errors that occur during schema validation.
type SchemaValidationError struct {
	KafkaError
	Schema       string
	ErrorDetails string
}

func NewSchemaValidationError(message, schema, errorDetails string) *SchemaValidationError {
	return &SchemaValidationError{
		KafkaError:   KafkaError{Message: message},
		Schema:       schema,
		ErrorDetails: errorDetails,
	}
}

func (e *SchemaValidationError) Error() string {
	return fmt.Sprintf("schema validation error: %s - %s", e.Message, e.ErrorDetails)
}

// SchemaRegistryError represents errors that occur when interacting with Schema Registry.
type SchemaRegistryError struct {
	KafkaError
	Subject string
}

func NewSchemaRegistryError(message, subject string, cause error) *SchemaRegistryError {
	return &SchemaRegistryError{
		KafkaError: KafkaError{Message: message, Cause: cause},
		Subject:    subject,
	}
}

func (e *SchemaRegistryError) Error() string {
	return fmt.Sprintf("schema registry error for subject '%s': %s", e.Subject, e.KafkaError.Error())
}

