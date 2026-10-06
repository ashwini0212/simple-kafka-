package com.simplekafka.protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 * controllerId int32
 * brokers:    count int32 | count * (id int32 | host string | port int32)
 * topics:     count int32 | count * (name string | errorCode int16 |
 *               partitions: count int32 | count * (partition int32 | errorCode int16 | leader int32 |
 *                                                   leaderEpoch int32 | replicas int[] | isr int[]))
 * </pre>
 */
public final class MetadataResponse implements Writable {
    private final int controllerId;
    private final List<BrokerInfo> brokers;
    private final List<TopicMetadata> topics;

    public MetadataResponse(int controllerId, List<BrokerInfo> brokers, List<TopicMetadata> topics) {
        this.controllerId = controllerId;
        this.brokers = brokers;
        this.topics = topics;
    }

    public int controllerId() {
        return controllerId;
    }

    public List<BrokerInfo> brokers() {
        return brokers;
    }

    public List<TopicMetadata> topics() {
        return topics;
    }

    @Override
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(controllerId);
        out.writeInt(brokers.size());
        for (BrokerInfo b : brokers) {
            b.writeTo(out);
        }
        out.writeInt(topics.size());
        for (TopicMetadata t : topics) {
            Protocol.writeString(out, t.name);
            out.writeShort(t.errorCode);
            out.writeInt(t.partitions.size());
            for (PartitionMetadata p : t.partitions) {
                out.writeInt(p.partition);
                out.writeShort(p.errorCode);
                out.writeInt(p.leader);
                out.writeInt(p.leaderEpoch);
                Protocol.writeIntList(out, p.replicas);
                Protocol.writeIntList(out, p.isr);
            }
        }
    }

    public static MetadataResponse readFrom(ByteBuffer buf) {
        int controllerId = buf.getInt();
        int brokerCount = buf.getInt();
        List<BrokerInfo> brokers = new ArrayList<>(brokerCount);
        for (int i = 0; i < brokerCount; i++) {
            brokers.add(BrokerInfo.readFrom(buf));
        }
        int topicCount = buf.getInt();
        List<TopicMetadata> topics = new ArrayList<>(topicCount);
        for (int i = 0; i < topicCount; i++) {
            String name = Protocol.readString(buf);
            short errorCode = buf.getShort();
            int partitionCount = buf.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>(partitionCount);
            for (int j = 0; j < partitionCount; j++) {
                int partition = buf.getInt();
                short pError = buf.getShort();
                int leader = buf.getInt();
                int leaderEpoch = buf.getInt();
                List<Integer> replicas = Protocol.readIntList(buf);
                List<Integer> isr = Protocol.readIntList(buf);
                partitions.add(new PartitionMetadata(partition, pError, leader, leaderEpoch, replicas, isr));
            }
            topics.add(new TopicMetadata(name, errorCode, partitions));
        }
        return new MetadataResponse(controllerId, brokers, topics);
    }

    public static final class TopicMetadata {
        public final String name;
        public final short errorCode;
        public final List<PartitionMetadata> partitions;

        public TopicMetadata(String name, short errorCode, List<PartitionMetadata> partitions) {
            this.name = name;
            this.errorCode = errorCode;
            this.partitions = partitions;
        }

        public PartitionMetadata partition(int id) {
            for (PartitionMetadata p : partitions) {
                if (p.partition == id) {
                    return p;
                }
            }
            return null;
        }
    }

    public static final class PartitionMetadata {
        public final int partition;
        public final short errorCode;
        public final int leader;
        public final int leaderEpoch;
        public final List<Integer> replicas;
        public final List<Integer> isr;

        public PartitionMetadata(int partition, short errorCode, int leader, int leaderEpoch,
                                 List<Integer> replicas, List<Integer> isr) {
            this.partition = partition;
            this.errorCode = errorCode;
            this.leader = leader;
            this.leaderEpoch = leaderEpoch;
            this.replicas = replicas;
            this.isr = isr;
        }

        @Override
        public String toString() {
            return "partition " + partition + ": leader=" + leader + " epoch=" + leaderEpoch
                    + " replicas=" + replicas + " isr=" + isr;
        }
    }
}
