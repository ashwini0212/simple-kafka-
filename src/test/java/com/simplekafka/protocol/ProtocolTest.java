package com.simplekafka.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ProtocolTest {
    private static ByteBuffer roundTrip(Writable w) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        w.writeTo(new DataOutputStream(bytes));
        return ByteBuffer.wrap(bytes.toByteArray());
    }

    @Test
    void produceRequestRoundTrip() throws Exception {
        ProduceRequest req = new ProduceRequest("orders", 2, ProduceRequest.ACKS_ALL, 5000,
                Arrays.asList(Message.of("k", "v1"), Message.of(null, "v2")));
        ProduceRequest decoded = ProduceRequest.readFrom(roundTrip(req));
        assertEquals("orders", decoded.topic());
        assertEquals(2, decoded.partition());
        assertEquals(ProduceRequest.ACKS_ALL, decoded.acks());
        assertEquals(2, decoded.messages().size());
        assertEquals("k", decoded.messages().get(0).keyAsString());
        assertNull(decoded.messages().get(1).key());
        assertEquals("v2", decoded.messages().get(1).valueAsString());
    }

    @Test
    void fetchResponseRoundTrip() throws Exception {
        FetchResponse resp = new FetchResponse(ErrorCode.NONE, 7, 9,
                Collections.singletonList(new Message(6, 123L, null, new byte[]{1, 2, 3})));
        FetchResponse decoded = FetchResponse.readFrom(roundTrip(resp));
        assertEquals(7, decoded.highWatermark());
        assertEquals(9, decoded.logEndOffset());
        assertEquals(6, decoded.messages().get(0).offset());
        assertEquals(123L, decoded.messages().get(0).timestamp());
        assertArrayEquals(new byte[]{1, 2, 3}, decoded.messages().get(0).value());
    }

    @Test
    void metadataResponseRoundTrip() throws Exception {
        MetadataResponse resp = new MetadataResponse(2,
                Arrays.asList(new BrokerInfo(1, "localhost", 9091), new BrokerInfo(2, "localhost", 9092)),
                Collections.singletonList(new MetadataResponse.TopicMetadata("t", ErrorCode.NONE,
                        Collections.singletonList(new MetadataResponse.PartitionMetadata(0, ErrorCode.NONE, 1, 3,
                                Arrays.asList(1, 2), Collections.singletonList(1))))));
        MetadataResponse decoded = MetadataResponse.readFrom(roundTrip(resp));
        assertEquals(2, decoded.controllerId());
        assertEquals(9092, decoded.brokers().get(1).port());
        MetadataResponse.PartitionMetadata p = decoded.topics().get(0).partition(0);
        assertEquals(1, p.leader);
        assertEquals(3, p.leaderEpoch);
        assertEquals(Arrays.asList(1, 2), p.replicas);
        assertEquals(Collections.singletonList(1), p.isr);
    }
}
