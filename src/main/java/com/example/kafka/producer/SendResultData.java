package com.example.kafka.producer;


/**
 * Result of a message send operation.
 */
public class SendResultData {

    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final String key;
    private final boolean success;
    private final Throwable error;

    private SendResultData(Builder builder) {
        this.topic = builder.topic;
        this.partition = builder.partition;
        this.offset = builder.offset;
        this.timestamp = builder.timestamp;
        this.key = builder.key;
        this.success = builder.success;
        this.error = builder.error;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static SendResultData success(String topic, int partition, long offset, long timestamp, String key) {
        return builder()
            .topic(topic)
            .partition(partition)
            .offset(offset)
            .timestamp(timestamp)
            .key(key)
            .success(true)
            .build();
    }

    public static SendResultData failure(String topic, String key, Throwable error) {
        return builder()
            .topic(topic)
            .key(key)
            .success(false)
            .error(error)
            .build();
    }

    // Getters
    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getKey() {
        return key;
    }

    public boolean isSuccess() {
        return success;
    }

    public Throwable getError() {
        return error;
    }

    @Override
    public String toString() {
        if (success) {
            return String.format("SendResult{success=true, topic='%s', partition=%d, offset=%d, timestamp=%d, key='%s'}",
                topic, partition, offset, timestamp, key);
        } else {
            return String.format("SendResult{success=false, topic='%s', key='%s', error='%s'}",
                topic, key, error != null ? error.getMessage() : "unknown");
        }
    }

    public static class Builder {
        private String topic;
        private int partition = -1;
        private long offset = -1;
        private long timestamp = -1;
        private String key;
        private boolean success = false;
        private Throwable error;

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder partition(int partition) {
            this.partition = partition;
            return this;
        }

        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder success(boolean success) {
            this.success = success;
            return this;
        }

        public Builder error(Throwable error) {
            this.error = error;
            return this;
        }

        public SendResultData build() {
            return new SendResultData(this);
        }
    }
}

