package com.simplekafka.broker;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Which brokers hold a replica of each partition, stored at
 * {@code /brokers/topics/<topic>} as {@code 0=1,2,3;1=2,3,1}.
 * The first replica listed is the partition's "preferred" leader.
 */
public final class TopicAssignment {
    private TopicAssignment() {
    }

    public static byte[] toZkData(Map<Integer, List<Integer>> assignment) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, List<Integer>> e : new TreeMap<>(assignment).entrySet()) {
            if (sb.length() > 0) {
                sb.append(';');
            }
            sb.append(e.getKey()).append('=');
            List<Integer> replicas = e.getValue();
            for (int i = 0; i < replicas.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(replicas.get(i));
            }
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static Map<Integer, List<Integer>> fromZkData(byte[] data) {
        Map<Integer, List<Integer>> assignment = new TreeMap<>();
        for (String entry : new String(data, StandardCharsets.UTF_8).split(";")) {
            String[] kv = entry.split("=", 2);
            if (kv.length == 2) {
                assignment.put(Integer.parseInt(kv[0]), PartitionState.parseIds(kv[1]));
            }
        }
        return assignment;
    }
}
