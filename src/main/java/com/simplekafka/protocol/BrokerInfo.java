package com.simplekafka.protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Where a broker can be reached. Stored in ZooKeeper and returned in metadata responses. */
public final class BrokerInfo {
    private final int id;
    private final String host;
    private final int port;

    public BrokerInfo(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public int id() {
        return id;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(id);
        Protocol.writeString(out, host);
        out.writeInt(port);
    }

    public static BrokerInfo readFrom(ByteBuffer buf) {
        int id = buf.getInt();
        String host = Protocol.readString(buf);
        int port = buf.getInt();
        return new BrokerInfo(id, host, port);
    }

    /** ZooKeeper registration format: {@code host:port}. */
    public byte[] toZkData() {
        return (host + ":" + port).getBytes(StandardCharsets.UTF_8);
    }

    public static BrokerInfo fromZkData(int id, byte[] data) {
        String s = new String(data, StandardCharsets.UTF_8);
        int colon = s.lastIndexOf(':');
        return new BrokerInfo(id, s.substring(0, colon), Integer.parseInt(s.substring(colon + 1)));
    }

    @Override
    public String toString() {
        return "broker " + id + " (" + host + ":" + port + ")";
    }
}
