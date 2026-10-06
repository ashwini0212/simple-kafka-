package com.simplekafka.broker;

import java.io.File;

/**
 * Broker settings. The first four come from the command line; the rest can be
 * overridden with -D system properties, e.g. {@code -Dsimplekafka.segment.bytes=4096}.
 */
public final class BrokerConfig {
    public final int brokerId;
    public final String host;
    public final int port;
    public final String zkConnect;
    public final File dataDir;
    public final int zkSessionTimeoutMs = Integer.getInteger("simplekafka.zk.session.timeout.ms", 6000);
    public final int segmentBytes = Integer.getInteger("simplekafka.segment.bytes", 1024 * 1024);
    public final int indexIntervalBytes = Integer.getInteger("simplekafka.index.interval.bytes", 4096);
    /** A follower that has not caught up for this long is dropped from the ISR. */
    public final long replicaLagTimeMs = Long.getLong("simplekafka.replica.lag.time.ms", 10_000L);
    public final int replicaFetchMaxBytes = 1024 * 1024;

    public BrokerConfig(int brokerId, String host, int port, String zkConnect) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.zkConnect = zkConnect;
        String base = System.getProperty("simplekafka.data.dir",
                new File(System.getProperty("java.io.tmpdir"), "simplekafka").getPath());
        this.dataDir = new File(base, "broker-" + brokerId);
    }
}
