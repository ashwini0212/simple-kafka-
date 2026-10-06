package com.simplekafka.broker;

import com.simplekafka.protocol.BrokerInfo;
import com.simplekafka.protocol.ErrorCode;
import com.simplekafka.protocol.MetadataResponse;
import com.simplekafka.protocol.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Every broker's local copy of cluster state, kept up to date from ZooKeeper
 * watches. It answers metadata requests without touching ZooKeeper.
 */
public class MetadataCache {
    volatile int controllerId = -1;
    final Map<Integer, BrokerInfo> liveBrokers = new ConcurrentHashMap<>();
    /** ZooKeeper creation id of each broker's registration, to spot a quick restart. */
    final Map<Integer, Long> brokerSessions = new ConcurrentHashMap<>();
    final Map<String, Map<Integer, List<Integer>>> assignments = new ConcurrentHashMap<>();
    final Map<TopicPartition, PartitionState> states = new ConcurrentHashMap<>();

    public List<Integer> replicas(TopicPartition tp) {
        Map<Integer, List<Integer>> a = assignments.get(tp.topic());
        if (a == null) {
            return Collections.emptyList();
        }
        List<Integer> replicas = a.get(tp.partition());
        return replicas == null ? Collections.emptyList() : replicas;
    }

    public MetadataResponse buildMetadata(List<String> requestedTopics) {
        List<BrokerInfo> brokers = new ArrayList<>(new TreeMap<>(liveBrokers).values());
        List<String> names = requestedTopics.isEmpty() ? new ArrayList<>(assignments.keySet()) : requestedTopics;
        Collections.sort(names);
        List<MetadataResponse.TopicMetadata> topics = new ArrayList<>();
        for (String name : names) {
            Map<Integer, List<Integer>> assignment = assignments.get(name);
            if (assignment == null) {
                topics.add(new MetadataResponse.TopicMetadata(name, ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                        Collections.emptyList()));
                continue;
            }
            List<MetadataResponse.PartitionMetadata> partitions = new ArrayList<>();
            for (Map.Entry<Integer, List<Integer>> e : new TreeMap<>(assignment).entrySet()) {
                PartitionState state = states.get(new TopicPartition(name, e.getKey()));
                int leader = state == null ? -1 : state.leader;
                if (!liveBrokers.containsKey(leader)) {
                    leader = -1;
                }
                short error = leader < 0 ? ErrorCode.LEADER_NOT_AVAILABLE : ErrorCode.NONE;
                partitions.add(new MetadataResponse.PartitionMetadata(e.getKey(), error, leader,
                        state == null ? -1 : state.leaderEpoch, e.getValue(),
                        state == null ? Collections.emptyList() : state.isr));
            }
            topics.add(new MetadataResponse.TopicMetadata(name, ErrorCode.NONE, partitions));
        }
        return new MetadataResponse(controllerId, brokers, topics);
    }
}
