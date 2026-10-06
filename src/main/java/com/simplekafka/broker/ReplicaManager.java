package com.simplekafka.broker;

import com.simplekafka.protocol.TopicPartition;
import com.simplekafka.storage.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Owns every partition replica hosted on this broker and reacts to leadership
 * changes: becoming leader stops fetching, becoming follower starts fetching
 * from the new leader.
 */
public class ReplicaManager {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaManager.class);

    private final BrokerConfig config;
    private final MetadataCache cache;
    private final ZkPartitionStates zkStates;
    private final Map<TopicPartition, Partition> partitions = new ConcurrentHashMap<>();
    private final Map<Integer, ReplicaFetcher> fetchers = new HashMap<>();

    public ReplicaManager(BrokerConfig config, MetadataCache cache, ZkPartitionStates zkStates) {
        this.config = config;
        this.cache = cache;
        this.zkStates = zkStates;
    }

    public Partition getPartition(TopicPartition tp) {
        return partitions.get(tp);
    }

    /** Called (on the ZooKeeper event thread) whenever a partition's state node changes. */
    public synchronized void onPartitionState(TopicPartition tp, List<Integer> replicas, PartitionState state)
            throws IOException {
        if (!replicas.contains(config.brokerId)) {
            return;
        }
        Partition partition = partitions.get(tp);
        if (partition == null) {
            Log log = new Log(new File(config.dataDir, tp.toString()), config.segmentBytes, config.indexIntervalBytes);
            partition = new Partition(tp, config.brokerId, log, zkStates, config.replicaLagTimeMs);
            partitions.put(tp, partition);
        }
        if (state.leader == config.brokerId) {
            assignFetcher(tp, -1);
            partition.makeLeader(state, replicas);
        } else {
            partition.makeFollower(state, replicas);
            assignFetcher(tp, state.leader);
        }
    }

    /** Moves tp to the fetcher for leaderId (or to none if leaderId &lt; 0). */
    private void assignFetcher(TopicPartition tp, int leaderId) {
        Iterator<Map.Entry<Integer, ReplicaFetcher>> it = fetchers.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, ReplicaFetcher> e = it.next();
            if (e.getKey() != leaderId) {
                e.getValue().partitions().remove(tp);
                if (e.getValue().partitions().isEmpty()) {
                    e.getValue().shutdown();
                    it.remove();
                }
            }
        }
        if (leaderId >= 0) {
            fetchers.computeIfAbsent(leaderId, id -> {
                ReplicaFetcher f = new ReplicaFetcher(config.brokerId, id, this, cache, config.replicaFetchMaxBytes);
                f.start();
                return f;
            }).partitions().add(tp);
        }
    }

    public void maybeShrinkIsrs() {
        for (Partition p : partitions.values()) {
            p.maybeShrinkIsr();
        }
    }

    public void checkpointHighWatermarks() {
        for (Partition p : partitions.values()) {
            try {
                p.log().checkpointHighWatermark();
            } catch (IOException e) {
                LOG.warn("Failed to checkpoint high watermark for {}: {}", p.topicPartition(), e.toString());
            }
        }
    }

    public synchronized void stopFetchers() {
        for (ReplicaFetcher f : fetchers.values()) {
            f.shutdown();
        }
        fetchers.clear();
    }

    public synchronized void closeLogs() {
        for (Partition p : partitions.values()) {
            try {
                p.log().close();
            } catch (IOException e) {
                LOG.warn("Failed to close log for {}: {}", p.topicPartition(), e.toString());
            }
        }
    }
}
