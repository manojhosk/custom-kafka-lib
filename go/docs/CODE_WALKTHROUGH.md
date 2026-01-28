## Code Walkthrough (Go Library)

This document walks through the main Go packages of the custom Kafka library and how
application teams are expected to use them.

The focus is on public APIs and the typical control flow; for a structural view of the
architecture, see `ARCHITECTURE.md` in the same directory.

---

## Entry Points for Application Teams

Most application teams will interact with just a few packages:

- `config`: to build configuration objects.
- `producer`: to send messages to Kafka.
- `consumer`: to read messages from Kafka and optionally forward them.
- `examples`: as reference implementations and starter code.

### Minimal Producer Example

In `go/examples/main.go`, the `runProducer` function shows the intended usage pattern:

```go
cfg := config.NewBuilder().
    BootstrapServers("localhost:9092").
    SchemaRegistryURL("http://localhost:8081").
    WithSchemaType(config.SchemaTypeAvro).
    Build()

p, err := producer.NewProducer(cfg)
if err != nil {
    // handle error
}
defer p.Close()

schemaID, err := p.RegisterSchema("users", userSchema)
if err != nil {
    // handle error
}

user := map[string]interface{}{
    "id":        "user-1",
    "email":     "user1@example.com",
    "name":      "User 1",
    "createdAt": time.Now().UnixMilli(),
}

result, err := p.Send("users", user["id"].(string), user)
if err != nil {
    // handle error
}
```

Key ideas:

- Configuration is built fluently.
- A schema is registered once per topic (`"users"`).
- Each `Send` call validates the payload against the registered schema before producing.

### Minimal Consumer Example

The `runConsumer` function in `go/examples/main.go` demonstrates the consumer:

```go
cfg := config.NewBuilder().
    BootstrapServers("localhost:9092").
    SchemaRegistryURL("http://localhost:8081").
    GroupID("example-consumer-group").
    AutoOffsetReset("earliest").
    EnableAutoCommit(true).
    Build()

c, err := consumer.NewConsumer(cfg)
if err != nil {
    // handle error
}

err = c.SubscribeSingle("users", func(msg *consumer.Message) error {
    log.Printf("Received message: key=%s, value=%v", msg.Key, msg.Value)
    return nil
})
if err != nil {
    // handle error
}

if err := c.Start(); err != nil {
    // handle error
}
```

Applications:

- Configure group ID and offset policy via the builder.
- Subscribe to one or more topics with a handler function.
- Call `Start()` (blocking) or `StartAsync()` to run the consume loop.

---

## Configuration (`config` Package)

File: `go/config/config.go`

### `Config` Structure

`Config` holds all core settings:

- Kafka connection: `BootstrapServers`.
- Schema Registry: `SchemaRegistryURL`.
- Schema type: `SchemaType` (either `SchemaTypeAvro` or `SchemaTypeJSON`).
- Producer settings: `Acks`, `Retries`, `RetryBackoffMs`, `DeliveryTimeoutMs`, `EnableIdempotence`.
- Consumer settings: `GroupID`, `AutoOffsetReset`, `EnableAutoCommit`,
  `AutoCommitIntervalMs`, `MaxPollRecords`.
- `AdditionalProperties`: extra Kafka client properties for advanced users.

### Builder Pattern

`NewBuilder()` returns a `Builder` with sensible defaults (idempotent producer, `acks=all`,
`auto.offset.reset=earliest`, etc.). Callers chain methods and finish with `Build()`:

```go
cfg := config.NewBuilder().
    BootstrapServers("localhost:9092").
    SchemaRegistryURL("http://localhost:8081").
    WithSchemaType(config.SchemaTypeJSON).
    GroupID("my-group").
    Acks("all").
    Retries(5).
    EnableIdempotence(true).
    Build()
```

The resulting `*Config` is passed to `producer.NewProducer` or `consumer.NewConsumer`.

---

## Producer (`producer` Package)

File: `go/producer/producer.go`

### Construction (`NewProducer`)

`NewProducer(cfg *config.Config)`:

- Builds a `kafka.ConfigMap` using:
  - `bootstrap.servers`
  - `acks`
  - `retries`
  - `retry.backoff.ms`
  - `delivery.timeout.ms`
  - `enable.idempotence`
  - Any `AdditionalProperties`.
- Constructs an underlying `*kafka.Producer`.
- Chooses a `schema.Validator` implementation based on `cfg.SchemaType` using
  `schema.NewValidatorFactory()`.
- Constructs a `registry.Manager` to talk to Schema Registry.

The resulting `Producer` instance keeps references to:

- The original `*config.Config`.
- The Kafka producer.
- The registry manager.
- The validator.
- An in-memory `schemaCache` keyed by topic.

### Registering Schemas

`RegisterSchema(topic, schemaStr string) (int, error)`:

- Derives the subject name as `{topic}-value` (e.g., `"users-value"`).
- Calls `registry.Manager.RegisterSchema(subject, schemaStr)` which:
  - Sends an HTTP POST to Schema Registry.
  - Returns a global schema ID.
- Caches the raw `schemaStr` in `Producer.schemaCache[topic]`.

This method should be called once per topic at startup; subsequent calls to `Send` rely on
the cached schema.

### Sending Messages

The main synchronous API:

- `Send(topic, key string, value interface{}) (*SendResult, error)`
- `SendWithHeaders(topic, key string, value interface{}, headers map[string]string)`

The flow:

1. `validateMessage(topic, value)`:
   - Looks up the schema string from `schemaCache` using the topic.
   - If no schema is found, returns a `SchemaValidationError`.
   - Delegates to the `schema.Validator` implementation:
     - `AvroValidator` for Avro.
     - `JSONValidator` for JSON Schema.
2. On success:
   - Marshals `value` to JSON bytes.
   - Builds a `kafka.Message` (including key and headers).
   - Produces the message to Kafka and waits for delivery on a channel.
3. Returns a `SendResult` with topic, partition, offset, key, and timestamp.

For asynchronous usage:

- `SendAsync(topic, key string, value interface{}, callback func(*SendResult, error)) error`
- `SendAsyncWithHeaders(...)`

These start a goroutine that:

- Performs the same validation and JSON encoding.
- Produces the message and waits for delivery.
- Invokes the callback with the `SendResult` or an error.

### Lifecycle

- `Flush(timeoutMs int)` delegates to the underlying Kafka producer to wait for in-flight messages.
- `Close()`:
  - Flushes remaining messages with a default timeout.
  - Closes the Kafka producer.
  - Marks the `Producer` as closed to prevent further sends.

---

## Consumer (`consumer` Package)

File: `go/consumer/consumer.go`

### Construction (`NewConsumer`)

`NewConsumer(cfg *config.Config)`:

- Builds a `kafka.ConfigMap` using:
  - `bootstrap.servers`
  - `group.id`
  - `auto.offset.reset`
  - `enable.auto.commit`
  - `auto.commit.interval.ms`
  - `max.poll.interval.ms`
  - Any `AdditionalProperties`.
- Creates an underlying `*kafka.Consumer`.
- Constructs a `registry.Manager`.
- Sets default `ErrorHandler` and `FatalErrorHandler`.
- Creates a background context and cancel function for the consume loop.

The resulting `Consumer` tracks:

- The underlying Kafka consumer.
- The message handler function (if configured).
- The error and fatal error handlers.
- An optional `HTTPForwarder`.
- A wait group and context for graceful shutdown.

### Subscribing and Starting

`Subscribe(topics []string, handler MessageHandler) error`:

- Stores `handler` on the `Consumer`.
- Calls `SubscribeTopics` on the underlying Kafka consumer.

`SubscribeSingle(topic string, handler MessageHandler) error`:

- Shortcut for single-topic subscriptions.

`Start()`:

- Validates that either a `MessageHandler` or an `HTTPForwarder` is configured.
- Runs the consume loop synchronously (blocking).

`StartAsync()`:

- Same validation as `Start()`.
- Launches the consume loop in a background goroutine and returns immediately.

### Consume Loop and Message Handling

The internal `consumeLoop`:

- Reads messages with `ReadMessage(100 * time.Millisecond)` in a loop.
- Ignores timeout errors and continues.
- On a real message:
  - Converts Kafka headers to `map[string]string`.
  - Attempts to `json.Unmarshal` the value into `interface{}`; falls back to a string if parsing fails.
  - Wraps everything into a `consumer.Message` struct (topic, partition, offset, timestamp, key, value, headers).

Processing pipeline for each `consumer.Message`:

- If a `MessageHandler` is set:
  - Calls `handler(msg)`.
  - If `handler` returns an error, calls `ErrorHandler(msg, err)`.
- If an `HTTPForwarder` is set:
  - Calls `httpForwarder.Forward(msg)`.
  - If forwarding fails, calls `ErrorHandler(msg, err)`.

If auto-commit is disabled (`EnableAutoCommit == false`), the consumer manually commits the
message after processing via `CommitMessage`.

### Stopping and Closing

- `Stop()`:
  - Cancels the consume loop context.
  - Waits for the consume goroutine to finish via the wait group.
- `Close()`:
  - Calls `Stop()`.
  - Closes the `HTTPForwarder` if present.
  - Closes the underlying Kafka consumer.

---

## HTTP Forwarding (`HTTPForwarder`)

File: `go/consumer/http_forwarder.go`

`HTTPForwarder` is a utility that sends message payloads to an HTTP endpoint, typically
used to connect Kafka to systems like Elasticsearch or custom APIs.

### Builder Usage

```go
forwarder := consumer.NewHTTPForwarder("http://localhost:9200/users/_doc").
    MaxRetries(3).
    RetryDelay(1000).
    Header("Content-Type", "application/json").
    ConnectTimeout(30 * time.Second).
    Build()

c.ForwardTo(forwarder)
```

Builder methods:

- `MaxRetries(retries int)`
- `RetryDelay(ms int64)`
- `ConnectTimeout(timeout time.Duration)`
- `Header(key, value string)`
- `Build()`

### Forwarding Behavior

- `Forward(msg *Message)`:
  - Delegates to `ForwardPayload(msg.Value)`.
- `ForwardPayload(payload interface{})`:
  - Marshals `payload` to JSON.
  - Issues an HTTP POST to the configured destination URL.
  - Retries up to `maxRetries` times with an increasing delay.
  - Returns an error if all attempts fail.

`Close()`:

- Calls `CloseIdleConnections()` on the underlying HTTP client.

---

## Schema Validation (`schema` Package)

File: `go/schema/*.go`

The `schema` package defines a common `Validator` interface with two methods:

- `Validate(data interface{}, schemaStr string) error`:
  - Returns a `SchemaValidationError` on failure.
- `IsValid(data interface{}, schemaStr string) ValidationResult`:
  - Returns a simple struct with `Valid` and `Errors` fields.

Concrete implementations:

- `AvroValidator`:
  - Uses `goavro.NewCodec(schemaStr)` to compile the Avro schema.
  - Converts input data to a native Go representation (via JSON if necessary).
  - Calls `BinaryFromNative` as a form of validation.
- `JSONValidator`:
  - Uses `jsonschema.NewCompiler()` to compile the schema.
  - Parses the input data into `interface{}` (via JSON if needed).
  - Calls `schema.Validate(jsonData)`.

The `ValidatorFactory` chooses which validator to use based on `schemaType`:

- `"AVRO"` → `NewAvroValidator()`
- `"JSON"` → `NewJSONValidator()`

---

## Schema Registry (`registry` Package)

File: `go/registry/manager.go`

`Manager` encapsulates HTTP access to Schema Registry:

- `RegisterSchema(subject, schemaStr string) (int, error)`:
  - Sends a POST to `/subjects/{subject}/versions` with schema and schema type.
  - Caches the schema string for the subject.
- `GetLatestSchema(subject string) (string, error)`:
  - Uses cache if available; otherwise calls `/subjects/{subject}/versions/latest`.
- `GetSchemaByVersion(subject string, version int) (string, error)`:
  - Calls `/subjects/{subject}/versions/{version}`.
- `GetSchemaByID(id int) (string, error)`:
  - Calls `/schemas/ids/{id}`.
- `GetAllVersions(subject string) ([]int, error)`:
  - Calls `/subjects/{subject}/versions`.
- `CheckCompatibility(subject, schemaStr string) (bool, error)`:
  - Calls `/compatibility/subjects/{subject}/versions/latest`.

The manager uses a simple in-memory map as a cache and a standard `http.Client` for requests.

---

## Error Types (`errors` Package)

File: `go/errors/errors.go`

Defines a base `KafkaError` and several specializations:

- `ProducerError`: wraps a message, cause error, and topic.
- `ConsumerError`: wraps a message, cause error, and topic.
- `SchemaValidationError`: used when validation fails; includes the schema and error details.
- `SchemaRegistryError`: used for registry-related failures; includes the subject.

These types implement `error` and carry additional context useful for logging, metrics, and
automated handling.

---

## How Everything Fits Together

Putting it all together, a typical application:

1. **Defines a schema** (Avro or JSON Schema).
2. **Starts producer**:
   - Builds `config.Config`.
   - Creates `producer.Producer`.
   - Registers schema for its topic(s).
   - Sends only messages that conform to that schema.
3. **Starts consumer(s)**:
   - Builds `config.Config` for a consumer group.
   - Creates `consumer.Consumer`.
   - Subscribes to topic(s) with a handler and/or HTTP forwarder.
   - Processes messages or forwards them to external systems.

The library’s abstractions aim to keep these steps consistent across all teams while
allowing deeper customization when needed.

