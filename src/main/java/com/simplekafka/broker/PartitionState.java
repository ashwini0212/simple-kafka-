package com.simplekafka.broker;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Leadership state of one partition, stored in ZooKeeper at
 * {@code /brokers/topics/<t>/partitions/<p>/state} as {@code leader=1;epoch=0;isr=1,2,3}.
 *
 * <ul>
 *   <li>leader: broker that serves reads and writes (-1 if none is alive)</li>
 *   <li>epoch: bumped on every leader change, so stale leaders can be told apart</li>
 *   <li>isr: in-sync replicas, the brokers that have every committed message</li>
 *   <li>zkVersion: the ZooKeeper node version, used for compare-and-set updates</li>
 * </ul>
 */
public final class PartitionState {
    public final int leader;
    public final int leaderEpoch;
    public final List<Integer> isr;
    public final int zkVersion;

    public PartitionState(int leader, int leaderEpoch, List<Integer> isr, int zkVersion) {
        this.leader = leader;
        this.leaderEpoch = leaderEpoch;
        this.isr = Collections.unmodifiableList(new ArrayList<>(isr));
        this.zkVersion = zkVersion;
    }

    public PartitionState withIsr(List<Integer> newIsr) {
        return new PartitionState(leader, leaderEpoch, newIsr, zkVersion);
    }

    public PartitionState withNewLeader(int newLeader, List<Integer> newIsr) {
        return new PartitionState(newLeader, leaderEpoch + 1, newIsr, zkVersion);
    }

    public byte[] toZkData() {
        StringBuilder sb = new StringBuilder();
        sb.append("leader=").append(leader).append(";epoch=").append(leaderEpoch).append(";isr=");
        for (int i = 0; i < isr.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(isr.get(i));
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static PartitionState fromZkData(byte[] data, int zkVersion) {
        int leader = -1;
        int epoch = 0;
        List<Integer> isr = new ArrayList<>();
        for (String field : new String(data, StandardCharsets.UTF_8).split(";")) {
            String[] kv = field.split("=", 2);
            if (kv.length != 2) {
                continue;
            }
            switch (kv[0]) {
                case "leader":
                    leader = Integer.parseInt(kv[1]);
                    break;
                case "epoch":
                    epoch = Integer.parseInt(kv[1]);
                    break;
                case "isr":
                    isr = parseIds(kv[1]);
                    break;
                default:
                    break;
            }
        }
        return new PartitionState(leader, epoch, isr, zkVersion);
    }

    static List<Integer> parseIds(String csv) {
        List<Integer> ids = new ArrayList<>();
        for (String s : csv.split(",")) {
            if (!s.isEmpty()) {
                ids.add(Integer.parseInt(s.trim()));
            }
        }
        return ids;
    }

    /** Same leader, epoch and ISR (ignores zkVersion). */
    public boolean sameAs(PartitionState other) {
        return other != null && leader == other.leader && leaderEpoch == other.leaderEpoch && isr.equals(other.isr);
    }

    @Override
    public String toString() {
        return "leader=" + leader + " epoch=" + leaderEpoch + " isr=" + isr;
    }
}
