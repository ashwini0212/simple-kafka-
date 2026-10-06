package com.simplekafka.zk;

/**
 * Where SimpleKafka keeps its cluster state in ZooKeeper (the same layout real
 * Kafka used before KRaft):
 *
 * <pre>
 * /brokers/ids/&lt;id&gt;                                  ephemeral, "host:port"    live brokers
 * /controller                                        ephemeral, "&lt;id&gt;"          who is controller
 * /brokers/topics/&lt;topic&gt;                            "0=1,2,3;1=2,3,1"          replica assignment
 * /brokers/topics/&lt;topic&gt;/partitions/&lt;p&gt;/state     "leader=1;epoch=0;isr=1,2,3"
 * </pre>
 *
 * Ephemeral nodes vanish when the broker's ZooKeeper session ends, which is how
 * the cluster notices a dead broker.
 */
public final class ZkPaths {
    public static final String BROKERS = "/brokers";
    public static final String BROKER_IDS = "/brokers/ids";
    public static final String TOPICS = "/brokers/topics";
    public static final String CONTROLLER = "/controller";

    private ZkPaths() {
    }

    public static String broker(int id) {
        return BROKER_IDS + "/" + id;
    }

    public static String topic(String topic) {
        return TOPICS + "/" + topic;
    }

    public static String partitions(String topic) {
        return topic(topic) + "/partitions";
    }

    public static String partition(String topic, int partition) {
        return partitions(topic) + "/" + partition;
    }

    public static String partitionState(String topic, int partition) {
        return partition(topic, partition) + "/state";
    }
}
