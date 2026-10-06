package com.simplekafka.protocol;

import java.util.Objects;

/** A (topic, partition) pair: the unit of ordering, storage and replication. */
public final class TopicPartition {
    private final String topic;
    private final int partition;

    public TopicPartition(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TopicPartition)) {
            return false;
        }
        TopicPartition other = (TopicPartition) o;
        return partition == other.partition && topic.equals(other.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition);
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
