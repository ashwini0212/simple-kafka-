package com.simplekafka.client;

import com.simplekafka.protocol.ErrorCode;

/** A broker answered with an error code. */
public class SimpleKafkaException extends RuntimeException {
    private final short errorCode;

    public SimpleKafkaException(short errorCode, String message) {
        super(ErrorCode.name(errorCode) + ": " + message);
        this.errorCode = errorCode;
    }

    public short errorCode() {
        return errorCode;
    }

    public boolean isRetriable() {
        return ErrorCode.isRetriable(errorCode);
    }
}
