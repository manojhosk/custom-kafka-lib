// Package registry provides Schema Registry client functionality.
package registry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/example/custom-kafka-lib/config"
	"github.com/example/custom-kafka-lib/errors"
)

// SchemaMetadata holds metadata about a schema.
type SchemaMetadata struct {
	ID      int    `json:"id"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
	Subject string `json:"subject"`
}

// Manager handles interactions with Schema Registry.
type Manager struct {
	baseURL    string
	schemaType config.SchemaType
	httpClient *http.Client
	cache      map[string]string
	cacheMu    sync.RWMutex
}

// NewManager creates a new Schema Registry Manager.
func NewManager(cfg *config.Config) *Manager {
	return &Manager{
		baseURL:    cfg.SchemaRegistryURL,
		schemaType: cfg.SchemaType,
		httpClient: &http.Client{},
		cache:      make(map[string]string),
	}
}

// ValueSubject returns the subject name for a topic's value schema.
func ValueSubject(topic string) string {
	return topic + "-value"
}

// KeySubject returns the subject name for a topic's key schema.
func KeySubject(topic string) string {
	return topic + "-key"
}

// RegisterSchema registers a schema for a given subject.
func (m *Manager) RegisterSchema(subject, schemaStr string) (int, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions", m.baseURL, subject)

	// Determine schema type for the request
	schemaType := "AVRO"
	if m.schemaType == config.SchemaTypeJSON {
		schemaType = "JSON"
	}

	payload := map[string]string{
		"schema":     schemaStr,
		"schemaType": schemaType,
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return 0, errors.NewSchemaRegistryError("failed to marshal schema payload", subject, err)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return 0, errors.NewSchemaRegistryError("failed to create request", subject, err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return 0, errors.NewSchemaRegistryError("failed to register schema", subject, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, errors.NewSchemaRegistryError(
			fmt.Sprintf("schema registry returned status %d: %s", resp.StatusCode, string(body)),
			subject,
			nil,
		)
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, errors.NewSchemaRegistryError("failed to decode response", subject, err)
	}

	// Cache the schema
	m.cacheMu.Lock()
	m.cache[subject] = schemaStr
	m.cacheMu.Unlock()

	return result.ID, nil
}

// GetLatestSchema retrieves the latest schema for a subject.
func (m *Manager) GetLatestSchema(subject string) (string, error) {
	// Check cache first
	m.cacheMu.RLock()
	if schema, ok := m.cache[subject]; ok {
		m.cacheMu.RUnlock()
		return schema, nil
	}
	m.cacheMu.RUnlock()

	url := fmt.Sprintf("%s/subjects/%s/versions/latest", m.baseURL, subject)

	resp, err := m.httpClient.Get(url)
	if err != nil {
		return "", errors.NewSchemaRegistryError("failed to get latest schema", subject, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", errors.NewSchemaRegistryError(
			fmt.Sprintf("schema registry returned status %d: %s", resp.StatusCode, string(body)),
			subject,
			nil,
		)
	}

	var metadata SchemaMetadata
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return "", errors.NewSchemaRegistryError("failed to decode response", subject, err)
	}

	// Cache the schema
	m.cacheMu.Lock()
	m.cache[subject] = metadata.Schema
	m.cacheMu.Unlock()

	return metadata.Schema, nil
}

// GetSchemaByVersion retrieves a specific version of a schema.
func (m *Manager) GetSchemaByVersion(subject string, version int) (string, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions/%d", m.baseURL, subject, version)

	resp, err := m.httpClient.Get(url)
	if err != nil {
		return "", errors.NewSchemaRegistryError("failed to get schema by version", subject, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", errors.NewSchemaRegistryError(
			fmt.Sprintf("schema registry returned status %d: %s", resp.StatusCode, string(body)),
			subject,
			nil,
		)
	}

	var metadata SchemaMetadata
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return "", errors.NewSchemaRegistryError("failed to decode response", subject, err)
	}

	return metadata.Schema, nil
}

// GetSchemaByID retrieves a schema by its global ID.
func (m *Manager) GetSchemaByID(id int) (string, error) {
	url := fmt.Sprintf("%s/schemas/ids/%d", m.baseURL, id)

	resp, err := m.httpClient.Get(url)
	if err != nil {
		return "", errors.NewSchemaRegistryError("failed to get schema by ID", "unknown", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", errors.NewSchemaRegistryError(
			fmt.Sprintf("schema registry returned status %d: %s", resp.StatusCode, string(body)),
			"unknown",
			nil,
		)
	}

	var result struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", errors.NewSchemaRegistryError("failed to decode response", "unknown", err)
	}

	return result.Schema, nil
}

// GetAllVersions retrieves all version numbers for a subject.
func (m *Manager) GetAllVersions(subject string) ([]int, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions", m.baseURL, subject)

	resp, err := m.httpClient.Get(url)
	if err != nil {
		return nil, errors.NewSchemaRegistryError("failed to get versions", subject, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, errors.NewSchemaRegistryError(
			fmt.Sprintf("schema registry returned status %d: %s", resp.StatusCode, string(body)),
			subject,
			nil,
		)
	}

	var versions []int
	if err := json.NewDecoder(resp.Body).Decode(&versions); err != nil {
		return nil, errors.NewSchemaRegistryError("failed to decode response", subject, err)
	}

	return versions, nil
}

// CheckCompatibility checks if a schema is compatible with the latest version.
func (m *Manager) CheckCompatibility(subject, schemaStr string) (bool, error) {
	url := fmt.Sprintf("%s/compatibility/subjects/%s/versions/latest", m.baseURL, subject)

	schemaType := "AVRO"
	if m.schemaType == config.SchemaTypeJSON {
		schemaType = "JSON"
	}

	payload := map[string]string{
		"schema":     schemaStr,
		"schemaType": schemaType,
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return false, errors.NewSchemaRegistryError("failed to marshal schema payload", subject, err)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return false, errors.NewSchemaRegistryError("failed to create request", subject, err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return false, errors.NewSchemaRegistryError("failed to check compatibility", subject, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return false, errors.NewSchemaRegistryError(
			fmt.Sprintf("schema registry returned status %d: %s", resp.StatusCode, string(body)),
			subject,
			nil,
		)
	}

	var result struct {
		IsCompatible bool `json:"is_compatible"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, errors.NewSchemaRegistryError("failed to decode response", subject, err)
	}

	return result.IsCompatible, nil
}

// ClearCache clears the schema cache.
func (m *Manager) ClearCache() {
	m.cacheMu.Lock()
	m.cache = make(map[string]string)
	m.cacheMu.Unlock()
}

// GetCachedSchema retrieves a schema from cache if available.
func (m *Manager) GetCachedSchema(subject string) (string, bool) {
	m.cacheMu.RLock()
	defer m.cacheMu.RUnlock()
	schema, ok := m.cache[subject]
	return schema, ok
}

// SetCachedSchema sets a schema in cache.
func (m *Manager) SetCachedSchema(subject, schema string) {
	m.cacheMu.Lock()
	m.cache[subject] = schema
	m.cacheMu.Unlock()
}

