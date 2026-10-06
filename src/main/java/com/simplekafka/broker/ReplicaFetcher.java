package com.simplekafka.broker;

import com.simplekafka.protocol.BrokerInfo;
import com.simplekafka.protocol.ErrorCode;
import com.simplekafka.protocol.FetchRequest;
import com.simplekafka.protocol.FetchResponse;
import com.simplekafka.protocol.KafkaConnection;
import com.simplekafka.protocol.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A background thread that copies data from one leader broker for every
 * partition this broker follows on it. Replication is pull-based: the follower
 * sends ordinary FETCH requests (with its broker id as replicaId) and appends
 * what comes back, keeping the leader's offsets.
 */
public class ReplicaFetcher extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaFetcher.class);
    private static final long IDLE_BACKOFF_MS = 50;
    private static final long ERROR_BACKOFF_MS = 500;

    private final int brokerId;
    private final int leaderId;
    private final ReplicaManager replicaManager;
    private final MetadataCache cache;
    private final int maxBytes;
    private final Set<TopicPartition> partitions = ConcurrentHashMap.newKeySet();
    private volatile boolean running = true;
    private KafkaConnection connection;
    private boolean warned;

    public ReplicaFetcher(int brokerId, int leaderId, ReplicaManager replicaManager, MetadataCache cache, int maxBytes) {
        super("replica-fetcher-" + brokerId + "-from-" + leaderId);
        setDaemon(true);
        this.brokerId = brokerId;
        this.leaderId = leaderId;
        this.replicaManager = replicaManager;
        this.cache = cache;
        this.maxBytes = maxBytes;
    }

    public Set<TopicPartition> partitions() {
        return partitions;
    }

    @Override
    public void run() {
        LOG.info("Started replica fetcher from broker {}", leaderId);
        while (running) {
            boolean gotData = false;
            try {
                for (TopicPartition tp : partitions) {
                    gotData |= fetchOnce(tp);
                }
                warned = false;
            } catch (IOException e) {
                if (!warned) {
                    LOG.warn("Cannot fetch from broker {}: {}", leaderId, e.toString());
                    warned = true;
                }
                closeConnection();
                sleepQuietly(ERROR_BACKOFF_MS);
                continue;
            }
            if (!gotData) {
                sleepQuietly(IDLE_BACKOFF_MS);
            }
        }
        closeConnection();
        LOG.info("Stopped replica fetcher from broker {}", leaderId);
    }

    private boolean fetchOnce(TopicPartition tp) throws IOException {
        Partition partition = replicaManager.getPartition(tp);
        if (partition == null) {
            return false;
        }
        PartitionState state = partition.state();
        if (state == null || partition.isLeader() || state.leader != leaderId) {
            return false;
        }
        long offset = partition.log().logEndOffset();
        FetchRequest request = new FetchRequest(brokerId, tp.topic(), tp.partition(), offset, maxBytes, 0);
        FetchResponse response = FetchResponse.readFrom(connection().send(request));
        if (response.errorCode() == ErrorCode.NONE) {
            partition.appendAsFollower(response.messages(), response.highWatermark(), leaderId, state.leaderEpoch);
            return !response.messages().isEmpty();
        }
        if (response.errorCode() == ErrorCode.OFFSET_OUT_OF_RANGE && offset > response.logEndOffset()) {
            partition.truncateAsFollower(response.logEndOffset(), leaderId, state.leaderEpoch);
        }
        // Other errors (e.g. NOT_LEADER) clear up once ZooKeeper tells us about the new leader.
        return false;
    }

    private KafkaConnection connection() throws IOException {
        if (connection == null) {
            BrokerInfo leader = cache.liveBrokers.get(leaderId);
            if (leader == null) {
                throw new IOException("broker " + leaderId + " is not registered");
            }
            connection = new KafkaConnection(leader.host(), leader.port(), "replica-fetcher-" + brokerId);
        }
        return connection;
    }

    private void closeConnection() {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            running = false;
        }
    }

    public void shutdown() {
        running = false;
        interrupt();
    }
}
