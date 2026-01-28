package com.example.kafka.registry;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.exception.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Client for interacting with Confluent Schema Registry.
 * Handles schema registration, retrieval, and version management.
 */
public class SchemaRegistryManager {

    private static final Logger logger = LoggerFactory.getLogger(SchemaRegistryManager.class);
    private static final int IDENTITY_MAP_CAPACITY = 1000;

    private final SchemaRegistryClient schemaRegistryClient;
    private final KafkaConfig.SchemaType schemaType;

    public SchemaRegistryManager(KafkaConfig config) {
        this.schemaRegistryClient = new CachedSchemaRegistryClient(
            config.getSchemaRegistryUrl(),
            IDENTITY_MAP_CAPACITY
        );
        this.schemaType = config.getSchemaType();
    }

    // Constructor for testing
    SchemaRegistryManager(SchemaRegistryClient client, KafkaConfig.SchemaType schemaType) {
        this.schemaRegistryClient = client;
        this.schemaType = schemaType;
    }

    /**
     * Register a schema for a given subject.
     *
     * @param subject The subject name (typically topic-value or topic-key)
     * @param schemaString The schema definition as a string
     * @return The schema ID assigned by the registry
     */
    public int registerSchema(String subject, String schemaString) {
        try {
            ParsedSchema parsedSchema = parseSchema(schemaString);
            int schemaId = schemaRegistryClient.register(subject, parsedSchema);
            logger.info("Registered schema for subject '{}' with ID: {}", subject, schemaId);
            return schemaId;
        } catch (IOException | RestClientException e) {
            throw new SchemaRegistryException(
                "Failed to register schema for subject: " + subject,
                subject,
                e
            );
        }
    }

    /**
     * Get the latest schema for a subject.
     *
     * @param subject The subject name
     * @return The schema string
     */
    public String getLatestSchema(String subject) {
        try {
            var metadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
            logger.debug("Retrieved latest schema for subject '{}', version: {}", subject, metadata.getVersion());
            return metadata.getSchema();
        } catch (IOException | RestClientException e) {
            throw new SchemaRegistryException(
                "Failed to get latest schema for subject: " + subject,
                subject,
                e
            );
        }
    }

    /**
     * Get a specific version of a schema.
     *
     * @param subject The subject name
     * @param version The schema version
     * @return The schema string
     */
    public String getSchemaByVersion(String subject, int version) {
        try {
            var metadata = schemaRegistryClient.getSchemaMetadata(subject, version);
            logger.debug("Retrieved schema for subject '{}', version: {}", subject, version);
            return metadata.getSchema();
        } catch (IOException | RestClientException e) {
            throw new SchemaRegistryException(
                "Failed to get schema for subject: " + subject + ", version: " + version,
                subject,
                e
            );
        }
    }

    /**
     * Get a schema by its global ID.
     *
     * @param schemaId The global schema ID
     * @return The parsed schema
     */
    public ParsedSchema getSchemaById(int schemaId) {
        try {
            return schemaRegistryClient.getSchemaById(schemaId);
        } catch (IOException | RestClientException e) {
            throw new SchemaRegistryException(
                "Failed to get schema by ID: " + schemaId,
                "unknown",
                e
            );
        }
    }

    /**
     * Get all versions of a schema for a subject.
     *
     * @param subject The subject name
     * @return List of version numbers
     */
    public List<Integer> getAllVersions(String subject) {
        try {
            return schemaRegistryClient.getAllVersions(subject);
        } catch (IOException | RestClientException e) {
            throw new SchemaRegistryException(
                "Failed to get versions for subject: " + subject,
                subject,
                e
            );
        }
    }

    /**
     * Get the latest version number for a subject.
     *
     * @param subject The subject name
     * @return The latest version number
     */
    public int getLatestVersion(String subject) {
        try {
            return schemaRegistryClient.getLatestSchemaMetadata(subject).getVersion();
        } catch (IOException | RestClientException e) {
            throw new SchemaRegistryException(
                "Failed to get latest version for subject: " + subject,
                subject,
                e
            );
        }
    }

    /**
     * Check if a schema is compatible with the latest version.
     *
     * @param subject The subject name
     * @param schemaString The new schema to check
     * @return true if compatible, false otherwise
     */
    public boolean isCompatible(String subject, String schemaString) {
        try {
            ParsedSchema parsedSchema = parseSchema(schemaString);
            boolean isCompatible = schemaRegistryClient.testCompatibility(subject, parsedSchema);
            if (!isCompatible) {
                logger.warn("Schema is not compatible with subject '{}'", subject);
            }
            return isCompatible;
        } catch (IOException | RestClientException e) {
            throw new SchemaRegistryException(
                "Failed to check compatibility for subject: " + subject,
                subject,
                e
            );
        }
    }

    /**
     * Check if a subject exists in the registry.
     *
     * @param subject The subject name
     * @return true if exists, false otherwise
     */
    public boolean subjectExists(String subject) {
        try {
            var subjects = schemaRegistryClient.getAllSubjects();
            return subjects.contains(subject);
        } catch (IOException | RestClientException e) {
            throw new SchemaRegistryException(
                "Failed to check if subject exists: " + subject,
                subject,
                e
            );
        }
    }

    /**
     * Get all registered subjects.
     *
     * @return Collection of subject names
     */
    public List<String> getAllSubjects() {
        try {
            var subjects = schemaRegistryClient.getAllSubjects();
            return subjects.stream().toList();
        } catch (IOException | RestClientException e) {
            throw new SchemaRegistryException(
                "Failed to get all subjects",
                "all",
                e
            );
        }
    }

    /**
     * Delete a subject and all its versions.
     *
     * @param subject The subject name
     * @return List of deleted versions
     */
    public List<Integer> deleteSubject(String subject) {
        try {
            return schemaRegistryClient.deleteSubject(subject);
        } catch (IOException | RestClientException e) {
            throw new SchemaRegistryException(
                "Failed to delete subject: " + subject,
                subject,
                e
            );
        }
    }

    /**
     * Parse a schema string into a ParsedSchema based on the configured schema type.
     */
    private ParsedSchema parseSchema(String schemaString) {
        return switch (schemaType) {
            case AVRO -> new AvroSchema(new Schema.Parser().parse(schemaString));
            case JSON_SCHEMA -> new JsonSchema(schemaString);
        };
    }

    /**
     * Get the schema registry client (for advanced use cases).
     */
    public SchemaRegistryClient getClient() {
        return schemaRegistryClient;
    }

    /**
     * Create subject name for a topic's value schema.
     *
     * @param topic The topic name
     * @return The subject name
     */
    public static String valueSubject(String topic) {
        return topic + "-value";
    }

    /**
     * Create subject name for a topic's key schema.
     *
     * @param topic The topic name
     * @return The subject name
     */
    public static String keySubject(String topic) {
        return topic + "-key";
    }
}

