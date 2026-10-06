package com.simplekafka.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A thin wrapper over the raw ZooKeeper API that turns the common
 * "node exists" / "no node" cases into return values instead of exceptions.
 *
 * Remember that ZooKeeper watches fire only once: whoever handles a watch
 * event must read the node again with a new watch to keep watching it.
 */
public class ZookeeperClient implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ZookeeperClient.class);

    private final String connectString;
    private final int sessionTimeoutMs;
    private final Runnable onSessionExpired;
    private ZooKeeper zk;

    public ZookeeperClient(String connectString, int sessionTimeoutMs, Runnable onSessionExpired) {
        this.connectString = connectString;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.onSessionExpired = onSessionExpired;
    }

    public void connect(long timeoutMs) throws IOException, InterruptedException {
        CountDownLatch connected = new CountDownLatch(1);
        zk = new ZooKeeper(connectString, sessionTimeoutMs, (WatchedEvent event) -> {
            switch (event.getState()) {
                case SyncConnected:
                    connected.countDown();
                    break;
                case Disconnected:
                    log.warn("Disconnected from ZooKeeper, client will try to reconnect");
                    break;
                case Expired:
                    log.error("ZooKeeper session expired");
                    onSessionExpired.run();
                    break;
                default:
                    break;
            }
        });
        if (!connected.await(timeoutMs, TimeUnit.MILLISECONDS)) {
            zk.close();
            throw new IOException("Could not connect to ZooKeeper at " + connectString + " within " + timeoutMs + " ms");
        }
    }

    /** Creates a persistent path and any missing parents. Existing nodes are left alone. */
    public void ensurePath(String path) throws KeeperException, InterruptedException {
        StringBuilder current = new StringBuilder();
        for (String part : path.substring(1).split("/")) {
            current.append('/').append(part);
            try {
                zk.create(current.toString(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ignored) {
                // already there
            }
        }
    }

    /** Returns false if the node already exists. */
    public boolean createEphemeral(String path, byte[] data) throws KeeperException, InterruptedException {
        try {
            zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            return true;
        } catch (KeeperException.NodeExistsException e) {
            return false;
        }
    }

    /** Runs several operations atomically: either all succeed or none do. */
    public void multi(List<Op> ops) throws KeeperException, InterruptedException {
        zk.multi(ops);
    }

    public static Op createOp(String path, byte[] data) {
        return Op.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /** Returns the node's data (filling in stat), or null if it does not exist. */
    public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        try {
            return zk.getData(path, watcher, stat);
        } catch (KeeperException.NoNodeException e) {
            return null;
        }
    }

    /** Returns the children, or an empty list if the node does not exist. */
    public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
        try {
            return zk.getChildren(path, watcher);
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyList();
        }
    }

    /** Sets a watch that fires when the node is created, changed or deleted. */
    public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return zk.exists(path, watcher);
    }

    /**
     * Conditional write: succeeds only if the node is still at expectedVersion.
     * Returns the new version, or -1 if someone else changed the node first.
     */
    public int setData(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException {
        try {
            return zk.setData(path, data, expectedVersion).getVersion();
        } catch (KeeperException.BadVersionException e) {
            return -1;
        }
    }

    public void delete(String path) throws KeeperException, InterruptedException {
        try {
            zk.delete(path, -1);
        } catch (KeeperException.NoNodeException ignored) {
            // already gone
        }
    }

    @Override
    public void close() {
        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
