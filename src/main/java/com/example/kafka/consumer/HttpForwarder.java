package com.example.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * HTTP forwarder for sending consumed messages to external APIs.
 * Handles retry logic and error handling.
 */
public class HttpForwarder implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(HttpForwarder.class);

    private final String destinationUrl;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final int maxRetries;
    private final long retryDelayMs;
    private final Map<String, String> defaultHeaders;

    private HttpForwarder(Builder builder) {
        this.destinationUrl = builder.destinationUrl;
        this.objectMapper = new ObjectMapper();
        this.maxRetries = builder.maxRetries;
        this.retryDelayMs = builder.retryDelayMs;
        this.defaultHeaders = builder.defaultHeaders;

        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(Timeout.ofMilliseconds(builder.connectTimeoutMs))
            .setResponseTimeout(Timeout.ofMilliseconds(builder.readTimeoutMs))
            .build();

        this.httpClient = HttpClients.custom()
            .setDefaultRequestConfig(requestConfig)
            .build();
    }

    public static Builder builder(String destinationUrl) {
        return new Builder(destinationUrl);
    }

    /**
     * Forward a message to the configured destination.
     *
     * @param message The message to forward
     * @return true if forwarding succeeded, false otherwise
     */
    public <V> boolean forward(ConsumedMessage<V> message) {
        return forwardWithPayload(message.getValue());
    }

    /**
     * Forward a payload to the configured destination.
     *
     * @param payload The payload to forward
     * @return true if forwarding succeeded, false otherwise
     */
    public boolean forwardWithPayload(Object payload) {
        int attempts = 0;
        Exception lastException = null;

        while (attempts < maxRetries) {
            try {
                doForward(payload);
                logger.debug("Successfully forwarded message to {}", destinationUrl);
                return true;
            } catch (Exception e) {
                lastException = e;
                attempts++;
                logger.warn("Failed to forward message (attempt {}/{}): {}",
                    attempts, maxRetries, e.getMessage());

                if (attempts < maxRetries) {
                    try {
                        Thread.sleep(retryDelayMs * attempts); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        logger.error("Failed to forward message after {} attempts", maxRetries, lastException);
        return false;
    }

    private void doForward(Object payload) throws IOException {
        HttpPost request = new HttpPost(destinationUrl);

        // Set default headers
        defaultHeaders.forEach(request::setHeader);
        request.setHeader("Content-Type", "application/json");

        // Set body
        String jsonPayload = convertToJson(payload);
        request.setEntity(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON));

        // Execute request
        httpClient.execute(request, response -> {
            int statusCode = response.getCode();
            if (statusCode < 200 || statusCode >= 300) {
                throw new IOException("HTTP request failed with status: " + statusCode);
            }
            return null;
        });
    }

    /**
     * Convert payload to JSON string using Avro's built-in JSON encoder for GenericRecord,
     * or Jackson for other types.
     */
    private String convertToJson(Object payload) throws IOException {
        if (payload == null) {
            return "null";
        }
        if (payload instanceof String) {
            return (String) payload;
        }
        if (payload instanceof GenericRecord record) {
            return avroToJson(record);
        }
        return objectMapper.writeValueAsString(payload);
    }

    /**
     * Convert Avro GenericRecord to JSON using Avro's built-in JsonEncoder.
     */
    private String avroToJson(GenericRecord record) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), outputStream);
        writer.write(record, encoder);
        encoder.flush();
        return outputStream.toString();
    }

    /**
     * Create a MessageHandler that forwards messages to this destination.
     *
     * @return A MessageHandler
     */
    public <V> MessageHandler<V> asMessageHandler() {
        return message -> {
            boolean success = forward(message);
            if (!success) {
                throw new RuntimeException("Failed to forward message to " + destinationUrl);
            }
        };
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }

    public static class Builder {
        private final String destinationUrl;
        private int maxRetries = 3;
        private long retryDelayMs = 1000;
        private int connectTimeoutMs = 5000;
        private int readTimeoutMs = 30000;
        private Map<String, String> defaultHeaders = new java.util.HashMap<>();

        public Builder(String destinationUrl) {
            this.destinationUrl = destinationUrl;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder retryDelayMs(long retryDelayMs) {
            this.retryDelayMs = retryDelayMs;
            return this;
        }

        public Builder connectTimeoutMs(int connectTimeoutMs) {
            this.connectTimeoutMs = connectTimeoutMs;
            return this;
        }

        public Builder readTimeoutMs(int readTimeoutMs) {
            this.readTimeoutMs = readTimeoutMs;
            return this;
        }

        public Builder header(String name, String value) {
            this.defaultHeaders.put(name, value);
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            this.defaultHeaders.putAll(headers);
            return this;
        }

        public HttpForwarder build() {
            if (destinationUrl == null || destinationUrl.isEmpty()) {
                throw new IllegalArgumentException("destinationUrl is required");
            }
            return new HttpForwarder(this);
        }
    }
}

