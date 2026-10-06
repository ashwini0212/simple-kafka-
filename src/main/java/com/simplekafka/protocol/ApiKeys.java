package com.simplekafka.protocol;

/**
 * The request types brokers understand. The api key is the first field of
 * every request header and tells the broker how to decode the body.
 */
public final class ApiKeys {
    public static final short PRODUCE = 0;
    public static final short FETCH = 1;
    public static final short METADATA = 2;
    public static final short CREATE_TOPIC = 3;

    private ApiKeys() {
    }

    public static String name(short apiKey) {
        switch (apiKey) {
            case PRODUCE: return "PRODUCE";
            case FETCH: return "FETCH";
            case METADATA: return "METADATA";
            case CREATE_TOPIC: return "CREATE_TOPIC";
            default: return "UNKNOWN(" + apiKey + ")";
        }
    }
}
