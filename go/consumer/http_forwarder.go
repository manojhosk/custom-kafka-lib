package consumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HTTPForwarder forwards consumed messages to an HTTP endpoint.
type HTTPForwarder struct {
	destinationURL string
	httpClient     *http.Client
	maxRetries     int
	retryDelayMs   int64
	defaultHeaders map[string]string
}

// HTTPForwarderBuilder provides a fluent API for building HTTPForwarder.
type HTTPForwarderBuilder struct {
	forwarder *HTTPForwarder
}

// NewHTTPForwarder creates a new HTTPForwarderBuilder.
func NewHTTPForwarder(destinationURL string) *HTTPForwarderBuilder {
	return &HTTPForwarderBuilder{
		forwarder: &HTTPForwarder{
			destinationURL: destinationURL,
			httpClient: &http.Client{
				Timeout: 30 * time.Second,
			},
			maxRetries:     3,
			retryDelayMs:   1000,
			defaultHeaders: make(map[string]string),
		},
	}
}

// MaxRetries sets the maximum number of retries.
func (b *HTTPForwarderBuilder) MaxRetries(retries int) *HTTPForwarderBuilder {
	b.forwarder.maxRetries = retries
	return b
}

// RetryDelay sets the retry delay in milliseconds.
func (b *HTTPForwarderBuilder) RetryDelay(ms int64) *HTTPForwarderBuilder {
	b.forwarder.retryDelayMs = ms
	return b
}

// ConnectTimeout sets the HTTP client timeout.
func (b *HTTPForwarderBuilder) ConnectTimeout(timeout time.Duration) *HTTPForwarderBuilder {
	b.forwarder.httpClient.Timeout = timeout
	return b
}

// Header adds a default header to all requests.
func (b *HTTPForwarderBuilder) Header(key, value string) *HTTPForwarderBuilder {
	b.forwarder.defaultHeaders[key] = value
	return b
}

// Build creates the HTTPForwarder.
func (b *HTTPForwarderBuilder) Build() *HTTPForwarder {
	return b.forwarder
}

// Forward forwards a message to the configured destination.
func (f *HTTPForwarder) Forward(msg *Message) error {
	return f.ForwardPayload(msg.Value)
}

// ForwardPayload forwards a payload to the configured destination.
func (f *HTTPForwarder) ForwardPayload(payload interface{}) error {
	var lastErr error

	for attempt := 0; attempt < f.maxRetries; attempt++ {
		err := f.doForward(payload)
		if err == nil {
			return nil
		}

		lastErr = err
		fmt.Printf("Failed to forward message (attempt %d/%d): %v\n", attempt+1, f.maxRetries, err)

		if attempt < f.maxRetries-1 {
			// Exponential backoff
			sleepDuration := time.Duration(f.retryDelayMs*int64(attempt+1)) * time.Millisecond
			time.Sleep(sleepDuration)
		}
	}

	return fmt.Errorf("failed to forward message after %d attempts: %w", f.maxRetries, lastErr)
}

func (f *HTTPForwarder) doForward(payload interface{}) error {
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, f.destinationURL, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range f.defaultHeaders {
		req.Header.Set(k, v)
	}

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP request returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Close closes the HTTP forwarder (closes idle connections).
func (f *HTTPForwarder) Close() {
	f.httpClient.CloseIdleConnections()
}

