package com.simplekafka.broker;

import com.simplekafka.protocol.TopicPartition;
import com.simplekafka.zk.ZkPaths;
import com.simplekafka.zk.ZookeeperClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.function.UnaryOperator;

/**
 * Reads and updates partition state nodes. Two parties write them: the
 * controller (leader changes) and the partition leader (ISR changes). Updates
 * are compare-and-set on the node version, retried on conflict, so neither
 * overwrites a change it has not seen.
 */
public class ZkPartitionStates {
    private final ZookeeperClient zk;

    public ZkPartitionStates(ZookeeperClient zk) {
        this.zk = zk;
    }

    public PartitionState read(TopicPartition tp) throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        byte[] data = zk.getData(ZkPaths.partitionState(tp.topic(), tp.partition()), null, stat);
        return data == null ? null : PartitionState.fromZkData(data, stat.getVersion());
    }

    /**
     * Applies change to the current state. If change returns null the update is
     * abandoned and null is returned. Otherwise returns the state as written.
     */
    public PartitionState update(TopicPartition tp, UnaryOperator<PartitionState> change)
            throws KeeperException, InterruptedException {
        String path = ZkPaths.partitionState(tp.topic(), tp.partition());
        for (int attempt = 0; attempt < 10; attempt++) {
            PartitionState current = read(tp);
            if (current == null) {
                return null;
            }
            PartitionState updated = change.apply(current);
            if (updated == null) {
                return null;
            }
            if (updated.sameAs(current)) {
                return current;
            }
            int newVersion = zk.setData(path, updated.toZkData(), current.zkVersion);
            if (newVersion >= 0) {
                return new PartitionState(updated.leader, updated.leaderEpoch, updated.isr, newVersion);
            }
        }
        throw new IllegalStateException("Could not update state of " + tp + " after repeated conflicts");
    }
}
