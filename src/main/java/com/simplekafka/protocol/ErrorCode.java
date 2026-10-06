package com.simplekafka.protocol;

/**
 * Error codes carried in responses. The numbers match real Kafka's codes so
 * they are easy to look up in the Kafka protocol guide.
 */
public final class ErrorCode {
    public static final short UNKNOWN_SERVER_ERROR = -1;
    public static final short NONE = 0;
    public static final short OFFSET_OUT_OF_RANGE = 1;
    public static final short CORRUPT_MESSAGE = 2;
    public static final short UNKNOWN_TOPIC_OR_PARTITION = 3;
    public static final short LEADER_NOT_AVAILABLE = 5;
    public static final short NOT_LEADER_FOR_PARTITION = 6;
    public static final short REQUEST_TIMED_OUT = 7;
    public static final short NOT_ENOUGH_REPLICAS = 19;
    public static final short TOPIC_ALREADY_EXISTS = 36;
    public static final short INVALID_PARTITIONS = 37;
    public static final short INVALID_REPLICATION_FACTOR = 38;
    public static final short NOT_CONTROLLER = 41;
    public static final short INVALID_REQUEST = 42;

    private ErrorCode() {
    }

    /** Errors that usually go away after refreshing metadata and trying again. */
    public static boolean isRetriable(short code) {
        return code == UNKNOWN_TOPIC_OR_PARTITION
                || code == LEADER_NOT_AVAILABLE
                || code == NOT_LEADER_FOR_PARTITION
                || code == NOT_CONTROLLER
                || code == NOT_ENOUGH_REPLICAS;
    }

    public static String name(short code) {
        switch (code) {
            case UNKNOWN_SERVER_ERROR: return "UNKNOWN_SERVER_ERROR";
            case NONE: return "NONE";
            case OFFSET_OUT_OF_RANGE: return "OFFSET_OUT_OF_RANGE";
            case CORRUPT_MESSAGE: return "CORRUPT_MESSAGE";
            case UNKNOWN_TOPIC_OR_PARTITION: return "UNKNOWN_TOPIC_OR_PARTITION";
            case LEADER_NOT_AVAILABLE: return "LEADER_NOT_AVAILABLE";
            case NOT_LEADER_FOR_PARTITION: return "NOT_LEADER_FOR_PARTITION";
            case REQUEST_TIMED_OUT: return "REQUEST_TIMED_OUT";
            case NOT_ENOUGH_REPLICAS: return "NOT_ENOUGH_REPLICAS";
            case TOPIC_ALREADY_EXISTS: return "TOPIC_ALREADY_EXISTS";
            case INVALID_PARTITIONS: return "INVALID_PARTITIONS";
            case INVALID_REPLICATION_FACTOR: return "INVALID_REPLICATION_FACTOR";
            case NOT_CONTROLLER: return "NOT_CONTROLLER";
            case INVALID_REQUEST: return "INVALID_REQUEST";
            default: return "ERROR(" + code + ")";
        }
    }
}
