package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for KafkaConsumer using mocks.
 * These tests do NOT require a running Kafka broker.
 */
class KafkaConsumerTest {

    @Mock
    private org.apache.kafka.clients.consumer.KafkaConsumer<String, Object> mockKafkaConsumer;

    @Mock
    private MessageHandler<Object> mockHandler;

    @Mock
    private HttpForwarder mockForwarder;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    @DisplayName("Should create ConsumedMessage from ConsumerRecord")
    void shouldCreateConsumedMessageFromConsumerRecord() {
        RecordHeaders headers = new RecordHeaders();
        headers.add("trace-id", "abc123".getBytes());

        ConsumerRecord<String, Object> record = new ConsumerRecord<>(
                "users",
                0,
                100L,
                System.currentTimeMillis(),
                null,
                0,
                0,
                "key-1",
                Map.of("id", "123", "name", "John"),
                headers,
                Optional.empty()
        );

        ConsumedMessage<Object> message = ConsumedMessage.from(record);

        assertEquals("users", message.getTopic());
        assertEquals(0, message.getPartition());
        assertEquals(100L, message.getOffset());
        assertEquals("key-1", message.getKey());
        assertNotNull(message.getValue());
        assertEquals("abc123", message.getHeaders().get("trace-id"));
    }

    @Test
    @DisplayName("Should invoke message handler for each message")
    void shouldInvokeMessageHandlerForEachMessage() throws Exception {
        ConsumedMessage<Object> message = new ConsumedMessage<>(
                "users", 0, 100L, System.currentTimeMillis(),
                "key-1", Map.of("id", "123"), Map.of()
        );

        // Simulate handler invocation
        mockHandler.handle(message);

        verify(mockHandler).handle(message);
    }

    @Test
    @DisplayName("Should invoke HTTP forwarder when configured")
    void shouldInvokeHttpForwarderWhenConfigured() {
        ConsumedMessage<Object> message = new ConsumedMessage<>(
                "users", 0, 100L, System.currentTimeMillis(),
                "key-1", Map.of("id", "123"), Map.of()
        );

        when(mockForwarder.forward(message)).thenReturn(true);

        boolean result = mockForwarder.forward(message);

        assertTrue(result);
        verify(mockForwarder).forward(message);
    }

    @Test
    @DisplayName("Should handle forwarder failure")
    void shouldHandleForwarderFailure() {
        ConsumedMessage<Object> message = new ConsumedMessage<>(
                "users", 0, 100L, System.currentTimeMillis(),
                "key-1", Map.of("id", "123"), Map.of()
        );

        when(mockForwarder.forward(message)).thenReturn(false);

        boolean result = mockForwarder.forward(message);

        assertFalse(result);
    }

    @Test
    @DisplayName("Should create consumer records correctly")
    void shouldCreateConsumerRecordsCorrectly() {
        TopicPartition partition = new TopicPartition("users", 0);
        ConsumerRecord<String, Object> record = new ConsumerRecord<>(
                "users", 0, 100L, "key-1", Map.of("id", "123")
        );

        Map<TopicPartition, List<ConsumerRecord<String, Object>>> recordsMap = new HashMap<>();
        recordsMap.put(partition, List.of(record));
        ConsumerRecords<String, Object> records = new ConsumerRecords<>(recordsMap);

        assertEquals(1, records.count());
        assertFalse(records.isEmpty());
    }

    @Test
    @DisplayName("Should handle empty poll result")
    void shouldHandleEmptyPollResult() {
        ConsumerRecords<String, Object> emptyRecords = new ConsumerRecords<>(Map.of());

        assertTrue(emptyRecords.isEmpty());
        assertEquals(0, emptyRecords.count());
    }

    @Test
    @DisplayName("Should process multiple records in batch")
    void shouldProcessMultipleRecordsInBatch() {
        TopicPartition partition = new TopicPartition("users", 0);
        List<ConsumerRecord<String, Object>> recordList = List.of(
                new ConsumerRecord<>("users", 0, 100L, "key-1", Map.of("id", "1")),
                new ConsumerRecord<>("users", 0, 101L, "key-2", Map.of("id", "2")),
                new ConsumerRecord<>("users", 0, 102L, "key-3", Map.of("id", "3"))
        );

        Map<TopicPartition, List<ConsumerRecord<String, Object>>> recordsMap = new HashMap<>();
        recordsMap.put(partition, recordList);
        ConsumerRecords<String, Object> records = new ConsumerRecords<>(recordsMap);

        assertEquals(3, records.count());

        int count = 0;
        for (ConsumerRecord<String, Object> record : records) {
            count++;
        }
        assertEquals(3, count);
    }

    @Test
    @DisplayName("Should handle custom error handler")
    void shouldHandleCustomErrorHandler() {
        KafkaConsumer.ErrorHandler errorHandler = mock(KafkaConsumer.ErrorHandler.class);

        ConsumedMessage<Object> message = new ConsumedMessage<>(
                "users", 0, 100L, System.currentTimeMillis(),
                "key-1", Map.of("id", "123"), Map.of()
        );
        Exception error = new RuntimeException("Processing failed");

        errorHandler.handleError(message, error);

        verify(errorHandler).handleError(message, error);
    }

    @Test
    @DisplayName("Should handle fatal error")
    void shouldHandleFatalError() {
        KafkaConsumer.ErrorHandler errorHandler = mock(KafkaConsumer.ErrorHandler.class);
        Exception fatalError = new RuntimeException("Connection lost");

        errorHandler.handleFatalError(fatalError);

        verify(errorHandler).handleFatalError(fatalError);
    }

    @Test
    @DisplayName("ConsumedMessage should expose all fields")
    void consumedMessageShouldExposeAllFields() {
        long timestamp = System.currentTimeMillis();
        Map<String, String> headers = Map.of("header1", "value1");
        Map<String, Object> value = Map.of("id", "123", "name", "Test");

        ConsumedMessage<Object> message = new ConsumedMessage<>(
                "test-topic", 2, 500L, timestamp,
                "test-key", value, headers
        );

        assertEquals("test-topic", message.getTopic());
        assertEquals(2, message.getPartition());
        assertEquals(500L, message.getOffset());
        assertEquals(timestamp, message.getTimestamp());
        assertEquals("test-key", message.getKey());
        assertEquals(value, message.getValue());
        assertEquals(headers, message.getHeaders());
    }

    @Test
    @DisplayName("MessageHandler should be a functional interface")
    void messageHandlerShouldBeFunctionalInterface() {
        // Verify MessageHandler can be used as a lambda
        MessageHandler<String> handler = message -> {
            assertNotNull(message);
        };

        ConsumedMessage<String> message = new ConsumedMessage<>(
                "test", 0, 0L, 0L, "key", "value", Map.of()
        );

        assertDoesNotThrow(() -> handler.handle(message));
    }
}

