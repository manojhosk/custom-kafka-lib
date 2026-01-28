package com.example.kafka.registry;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.exception.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for SchemaRegistryManager using mocks.
 * These tests do NOT require a running Schema Registry.
 */
class SchemaRegistryManagerTest {

    @Mock
    private SchemaRegistryClient mockClient;

    @Mock
    private SchemaMetadata mockMetadata;

    @Mock
    private ParsedSchema mockParsedSchema;

    private SchemaRegistryManager registryManager;

    private static final String AVRO_SCHEMA = """
        {
          "type": "record",
          "name": "User",
          "fields": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"}
          ]
        }
        """;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        registryManager = new SchemaRegistryManager(mockClient, KafkaConfig.SchemaType.AVRO);
    }

    @Test
    @DisplayName("Should register schema successfully")
    void shouldRegisterSchemaSuccessfully() throws Exception {
        when(mockClient.register(anyString(), any(ParsedSchema.class))).thenReturn(1);

        int schemaId = registryManager.registerSchema("users-value", AVRO_SCHEMA);

        assertEquals(1, schemaId);
        verify(mockClient).register(eq("users-value"), any(ParsedSchema.class));
    }

    @Test
    @DisplayName("Should throw SchemaRegistryException when registration fails")
    void shouldThrowExceptionWhenRegistrationFails() throws Exception {
        when(mockClient.register(anyString(), any(ParsedSchema.class)))
                .thenThrow(new RestClientException("Connection refused", 500, 50001));

        assertThrows(SchemaRegistryException.class, () ->
                registryManager.registerSchema("users-value", AVRO_SCHEMA));
    }

    @Test
    @DisplayName("Should get latest schema successfully")
    void shouldGetLatestSchemaSuccessfully() throws Exception {
        when(mockMetadata.getSchema()).thenReturn(AVRO_SCHEMA);
        when(mockMetadata.getVersion()).thenReturn(1);
        when(mockClient.getLatestSchemaMetadata("users-value")).thenReturn(mockMetadata);

        String schema = registryManager.getLatestSchema("users-value");

        assertEquals(AVRO_SCHEMA, schema);
        verify(mockClient).getLatestSchemaMetadata("users-value");
    }

    @Test
    @DisplayName("Should throw SchemaRegistryException when getting latest schema fails")
    void shouldThrowExceptionWhenGetLatestSchemaFails() throws Exception {
        when(mockClient.getLatestSchemaMetadata("users-value"))
                .thenThrow(new RestClientException("Not found", 404, 40401));

        assertThrows(SchemaRegistryException.class, () ->
                registryManager.getLatestSchema("users-value"));
    }

    @Test
    @DisplayName("Should get schema by version successfully")
    void shouldGetSchemaByVersionSuccessfully() throws Exception {
        when(mockMetadata.getSchema()).thenReturn(AVRO_SCHEMA);
        when(mockClient.getSchemaMetadata("users-value", 1)).thenReturn(mockMetadata);

        String schema = registryManager.getSchemaByVersion("users-value", 1);

        assertEquals(AVRO_SCHEMA, schema);
        verify(mockClient).getSchemaMetadata("users-value", 1);
    }

    @Test
    @DisplayName("Should get all versions successfully")
    void shouldGetAllVersionsSuccessfully() throws Exception {
        when(mockClient.getAllVersions("users-value")).thenReturn(List.of(1, 2, 3));

        List<Integer> versions = registryManager.getAllVersions("users-value");

        assertEquals(List.of(1, 2, 3), versions);
        verify(mockClient).getAllVersions("users-value");
    }

    @Test
    @DisplayName("Should check if subject exists")
    void shouldCheckIfSubjectExists() throws Exception {
        when(mockClient.getAllSubjects()).thenReturn(List.of("users-value", "orders-value"));

        boolean exists = registryManager.subjectExists("users-value");
        boolean notExists = registryManager.subjectExists("products-value");

        assertTrue(exists);
        assertFalse(notExists);
    }

    @Test
    @DisplayName("Should generate correct value subject name")
    void shouldGenerateCorrectValueSubjectName() {
        String subject = SchemaRegistryManager.valueSubject("users");
        assertEquals("users-value", subject);
    }

    @Test
    @DisplayName("Should generate correct key subject name")
    void shouldGenerateCorrectKeySubjectName() {
        String subject = SchemaRegistryManager.keySubject("users");
        assertEquals("users-key", subject);
    }

    @Test
    @DisplayName("Should test schema compatibility - compatible")
    void shouldTestSchemaCompatibilityCompatible() throws Exception {
        when(mockClient.testCompatibility(anyString(), any(ParsedSchema.class))).thenReturn(true);

        boolean compatible = registryManager.isCompatible("users-value", AVRO_SCHEMA);

        assertTrue(compatible);
        verify(mockClient).testCompatibility(eq("users-value"), any(ParsedSchema.class));
    }

    @Test
    @DisplayName("Should test schema compatibility - not compatible")
    void shouldTestSchemaCompatibilityNotCompatible() throws Exception {
        when(mockClient.testCompatibility(anyString(), any(ParsedSchema.class))).thenReturn(false);

        boolean compatible = registryManager.isCompatible("users-value", AVRO_SCHEMA);

        assertFalse(compatible);
    }

    @Test
    @DisplayName("Should throw exception when compatibility check fails")
    void shouldThrowExceptionWhenCompatibilityCheckFails() throws Exception {
        when(mockClient.testCompatibility(anyString(), any(ParsedSchema.class)))
                .thenThrow(new RestClientException("Error", 500, 50001));

        assertThrows(SchemaRegistryException.class, () ->
                registryManager.isCompatible("users-value", AVRO_SCHEMA));
    }
}

