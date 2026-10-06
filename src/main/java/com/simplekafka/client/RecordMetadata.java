package com.simplekafka.client;

/** Where a produced message ended up. */
public final class RecordMetadata {
    private final String topic;
    private final int partition;
    private final long offset;

    public RecordMetadata(String topic, int partition, long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    @Override
    public String toString() {
        return topic + "-" + partition + "@" + offset;
    }
}
