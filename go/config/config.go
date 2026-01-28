// Package config provides configuration for the Custom Kafka library.
package config

import "time"

// SchemaType represents the type of schema (AVRO or JSON).
type SchemaType string

const (
	SchemaTypeAvro SchemaType = "AVRO"
	SchemaTypeJSON SchemaType = "JSON"
)

// Config holds the configuration for Kafka producer and consumer.
type Config struct {
	// Core settings
	BootstrapServers  string
	SchemaRegistryURL string
	SchemaType        SchemaType

	// Producer settings
	Acks              string
	Retries           int
	RetryBackoffMs    int
	DeliveryTimeoutMs int
	EnableIdempotence bool

	// Consumer settings
	GroupID              string
	AutoOffsetReset      string
	EnableAutoCommit     bool
	AutoCommitIntervalMs int
	MaxPollRecords       int

	// Additional properties
	AdditionalProperties map[string]interface{}
}

// Builder provides a fluent API for building Config.
type Builder struct {
	config *Config
}

// NewBuilder creates a new Config builder with default values.
func NewBuilder() *Builder {
	return &Builder{
		config: &Config{
			SchemaType:           SchemaTypeAvro,
			Acks:                 "all",
			Retries:              3,
			RetryBackoffMs:       100,
			DeliveryTimeoutMs:    30000,
			EnableIdempotence:    true,
			AutoOffsetReset:      "earliest",
			EnableAutoCommit:     true,
			AutoCommitIntervalMs: 5000,
			MaxPollRecords:       500,
			AdditionalProperties: make(map[string]interface{}),
		},
	}
}

// BootstrapServers sets the Kafka bootstrap servers.
func (b *Builder) BootstrapServers(servers string) *Builder {
	b.config.BootstrapServers = servers
	return b
}

// SchemaRegistryURL sets the Schema Registry URL.
func (b *Builder) SchemaRegistryURL(url string) *Builder {
	b.config.SchemaRegistryURL = url
	return b
}

// WithSchemaType sets the schema type (AVRO or JSON).
func (b *Builder) WithSchemaType(schemaType SchemaType) *Builder {
	b.config.SchemaType = schemaType
	return b
}

// Acks sets the producer acks setting.
func (b *Builder) Acks(acks string) *Builder {
	b.config.Acks = acks
	return b
}

// Retries sets the number of retries for the producer.
func (b *Builder) Retries(retries int) *Builder {
	b.config.Retries = retries
	return b
}

// RetryBackoff sets the retry backoff in milliseconds.
func (b *Builder) RetryBackoff(ms int) *Builder {
	b.config.RetryBackoffMs = ms
	return b
}

// DeliveryTimeout sets the delivery timeout in milliseconds.
func (b *Builder) DeliveryTimeout(ms int) *Builder {
	b.config.DeliveryTimeoutMs = ms
	return b
}

// EnableIdempotence enables or disables idempotent producer.
func (b *Builder) EnableIdempotence(enable bool) *Builder {
	b.config.EnableIdempotence = enable
	return b
}

// GroupID sets the consumer group ID.
func (b *Builder) GroupID(groupID string) *Builder {
	b.config.GroupID = groupID
	return b
}

// AutoOffsetReset sets the auto offset reset policy.
func (b *Builder) AutoOffsetReset(policy string) *Builder {
	b.config.AutoOffsetReset = policy
	return b
}

// EnableAutoCommit enables or disables auto commit.
func (b *Builder) EnableAutoCommit(enable bool) *Builder {
	b.config.EnableAutoCommit = enable
	return b
}

// AutoCommitInterval sets the auto commit interval in milliseconds.
func (b *Builder) AutoCommitInterval(ms int) *Builder {
	b.config.AutoCommitIntervalMs = ms
	return b
}

// MaxPollRecords sets the maximum records to poll.
func (b *Builder) MaxPollRecords(records int) *Builder {
	b.config.MaxPollRecords = records
	return b
}

// AdditionalProperty adds an additional Kafka property.
func (b *Builder) AdditionalProperty(key string, value interface{}) *Builder {
	b.config.AdditionalProperties[key] = value
	return b
}

// Build creates the Config from the builder.
func (b *Builder) Build() *Config {
	return b.config
}

// GetDeliveryTimeout returns the delivery timeout as a time.Duration.
func (c *Config) GetDeliveryTimeout() time.Duration {
	return time.Duration(c.DeliveryTimeoutMs) * time.Millisecond
}

// GetRetryBackoff returns the retry backoff as a time.Duration.
func (c *Config) GetRetryBackoff() time.Duration {
	return time.Duration(c.RetryBackoffMs) * time.Millisecond
}

// GetAutoCommitInterval returns the auto commit interval as a time.Duration.
func (c *Config) GetAutoCommitInterval() time.Duration {
	return time.Duration(c.AutoCommitIntervalMs) * time.Millisecond
}

