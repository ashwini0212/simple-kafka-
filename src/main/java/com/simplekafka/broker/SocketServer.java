package com.simplekafka.broker;

import com.simplekafka.protocol.Protocol;
import com.simplekafka.protocol.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Accepts TCP connections and serves requests on them, one thread per
 * connection. (Real Kafka uses non-blocking NIO selectors and a request queue;
 * thread-per-connection is easier to follow and fine for a handful of clients.)
 *
 * Requests on one connection are processed strictly in order, which is what
 * gives clients per-connection ordering guarantees.
 */
public class SocketServer {
    private static final Logger LOG = LoggerFactory.getLogger(SocketServer.class);

    /** Decodes a request body and returns the response body. */
    public interface RequestHandler {
        Writable handle(short apiKey, String clientId, ByteBuffer body) throws Exception;
    }

    private final int port;
    private final RequestHandler handler;
    private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
    private final ExecutorService pool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "connection-handler");
        t.setDaemon(true);
        return t;
    });
    private ServerSocket serverSocket;
    private volatile boolean running;

    public SocketServer(int port, RequestHandler handler) {
        this.port = port;
        this.handler = handler;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(port));
        running = true;
        Thread acceptor = new Thread(this::acceptLoop, "acceptor-" + port);
        acceptor.setDaemon(true);
        acceptor.start();
        LOG.info("Listening on port {}", port);
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                connections.add(socket);
                pool.submit(() -> serve(socket));
            } catch (IOException e) {
                if (running) {
                    LOG.warn("Accept failed: {}", e.toString());
                }
            }
        }
    }

    private void serve(Socket socket) {
        try (Socket s = socket;
             DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()))) {
            while (running) {
                int size = in.readInt();
                if (size < 6 || size > Protocol.MAX_FRAME_BYTES) {
                    LOG.warn("Closing connection from {}: invalid frame size {}", s.getRemoteSocketAddress(), size);
                    return;
                }
                byte[] frame = new byte[size];
                in.readFully(frame);
                ByteBuffer buf = ByteBuffer.wrap(frame);
                short apiKey = buf.getShort();
                int correlationId = buf.getInt();
                String clientId = Protocol.readString(buf);

                Writable response = handler.handle(apiKey, clientId, buf);

                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream body = new DataOutputStream(bytes);
                body.writeInt(correlationId);
                response.writeTo(body);
                body.flush();
                out.writeInt(bytes.size());
                bytes.writeTo(out);
                out.flush();
            }
        } catch (EOFException | SocketException e) {
            // client went away
        } catch (Exception e) {
            if (running) {
                LOG.warn("Closing connection after error: {}", e.toString());
            }
        } finally {
            connections.remove(socket);
        }
    }

    public void shutdown() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
            // shutting down anyway
        }
        for (Socket s : connections) {
            try {
                s.close();
            } catch (IOException ignored) {
                // shutting down anyway
            }
        }
        pool.shutdownNow();
    }
}
