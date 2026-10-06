package com.simplekafka.protocol;

/** A request body; the api key goes into the request header. */
public interface Request extends Writable {
    short apiKey();
}
