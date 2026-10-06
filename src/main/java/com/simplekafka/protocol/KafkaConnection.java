package com.simplekafka.protocol;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * A blocking TCP connection to one broker, used by clients and by brokers
 * talking to each other (replication, forwarding to the controller).
 *
 * <pre>
 * request frame:  size int32 | apiKey int16 | correlationId int32 | clientId string | body
 * response frame: size int32 | correlationId int32 | body
 * </pre>
 *
 * The size prefix lets the receiver read exactly one message off the stream;
 * the correlation id pairs each response with its request.
 */
public class KafkaConnection implements Closeable {
    private static final int CONNECT_TIMEOUT_MS = 5000;
    private static final int READ_TIMEOUT_MS = 30000;

    private final String host;
    private final int port;
    private final String clientId;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private int nextCorrelationId = 0;

    public KafkaConnection(String host, int port, String clientId) {
        this.host = host;
        this.port = port;
        this.clientId = clientId;
    }

    public String address() {
        return host + ":" + port;
    }

    private void ensureConnected() throws IOException {
        if (socket != null && !socket.isClosed()) {
            return;
        }
        Socket s = new Socket();
        s.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
        s.setSoTimeout(READ_TIMEOUT_MS);
        s.setTcpNoDelay(true);
        socket = s;
        in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
    }

    /** Sends a request and blocks for its response; returns the response body. */
    public synchronized ByteBuffer send(Request request) throws IOException {
        ensureConnected();
        int correlationId = nextCorrelationId++;
        try {
            ByteArrayOutputStream frame = new ByteArrayOutputStream();
            DataOutputStream frameOut = new DataOutputStream(frame);
            frameOut.writeShort(request.apiKey());
            frameOut.writeInt(correlationId);
            Protocol.writeString(frameOut, clientId);
            request.writeTo(frameOut);
            frameOut.flush();

            out.writeInt(frame.size());
            frame.writeTo(out);
            out.flush();

            int size = in.readInt();
            if (size < 4 || size > Protocol.MAX_FRAME_BYTES) {
                throw new IOException("Invalid response size " + size + " from " + address());
            }
            byte[] response = new byte[size];
            in.readFully(response);
            ByteBuffer buf = ByteBuffer.wrap(response);
            int responseCorrelationId = buf.getInt();
            if (responseCorrelationId != correlationId) {
                throw new IOException("Expected correlation id " + correlationId + " but got " + responseCorrelationId);
            }
            return buf;
        } catch (IOException e) {
            close();
            throw e;
        }
    }

    @Override
    public synchronized void close() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // nothing useful to do
            }
            socket = null;
        }
    }
}
