package com.simplekafka.protocol;

import java.io.DataOutputStream;
import java.io.IOException;

/** Anything that can serialize itself onto the wire. */
public interface Writable {
    void writeTo(DataOutputStream out) throws IOException;
}
