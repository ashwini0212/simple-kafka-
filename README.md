# SimpleKafka

A simplified Kafka clone in Java, built from scratch to learn how Kafka works inside:
a binary protocol, ZooKeeper-based cluster coordination, segmented on-disk logs with
offset indices, controller election, leader/follower replication with an ISR and high
watermark, and producer/consumer clients.

About 4,000 lines of Java 11 including tests. The only runtime dependencies are the ZooKeeper client and SLF4J.

**Try it in your browser:** [`docs/index.html`](docs/index.html) is an interactive playground of the same
cluster (ZooKeeper, three brokers, controller, ISR). Enable GitHub Pages for the `docs/` folder and it is
served at `https://<your-user>.github.io/simple-kafka/`, or just open the file locally.

## Requirements

- Java 11 or newer
- Maven 3.6 or newer
- ZooKeeper (optional: the jar can run one for you, see below)

## Build

```bash
mvn clean package
```

This runs the unit tests and builds one runnable jar with everything inside:
`target/simple-kafka-1.0-SNAPSHOT.jar`.

## Run a three-broker cluster

**1. Start ZooKeeper.** If you have it installed, use `zkServer start`. Otherwise the jar
already contains ZooKeeper's server, so this works anywhere:

```bash
java -Dzookeeper.admin.enableServer=false -cp target/simple-kafka-1.0-SNAPSHOT.jar \
  org.apache.zookeeper.server.ZooKeeperServerMain 2181 /tmp/zookeeper
```

**2. Start three brokers**, one per terminal (arguments: id, host, port, ZooKeeper port):

```bash
java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.broker.SimpleKafkaBroker 1 localhost 9091 2181
java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.broker.SimpleKafkaBroker 2 localhost 9092 2181
java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.broker.SimpleKafkaBroker 3 localhost 9093 2181
```

The first broker to start wins the controller election and logs
`Broker 1 won the election and is now the CONTROLLER`.

**3. Produce.** Type lines; each one is a message. The topic is created on first use
with 3 partitions and replication factor 3, and the producer prints who leads each partition.

```bash
java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.client.SimpleKafkaProducer localhost 9091 test-topic
```

```
Topic test-topic:
  partition 0: leader=3 epoch=0 replicas=[3, 1, 2] isr=[3, 1, 2]
  partition 1: leader=1 epoch=0 replicas=[1, 2, 3] isr=[1, 2, 3]
  partition 2: leader=2 epoch=0 replicas=[2, 3, 1] isr=[2, 3, 1]
> hello
Sent to partition 0 at offset 0
```

**4. Consume** one partition (optional 5th argument: start offset, default 0):

```bash
java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.client.SimpleKafkaConsumer localhost 9091 test-topic 0
```

You can bootstrap from any broker; the client asks it for metadata and then talks to
each partition's leader directly.

**Data and logs.** Each broker stores its data in `/tmp/simplekafka/broker-<id>/<topic>-<partition>/`.
Delete `/tmp/simplekafka` and the ZooKeeper data dir to start fresh.

## Experiments to try

| Try this | What you should see |
|---|---|
| `kill -9` the broker leading a partition | About 6 seconds later (the ZooKeeper session timeout) the controller logs `leader 1 -> 2`, the new leader logs `became LEADER at epoch 1`, and producers and consumers retry against it. |
| `kill -9` the controller | The other brokers race to create `/controller`; one logs `won the election`. |
| Stop a broker with Ctrl+C instead | Same failover, but almost instantly: a clean shutdown closes the ZooKeeper session, deleting its ephemeral node right away. |
| Restart the stopped broker | It becomes a follower, truncates to its high watermark, catches up, and the leader logs `broker 1 caught up, expanding ISR [2, 3] -> [2, 3, 1]`. |
| Kill two of three brokers | The survivor leads every partition with `isr=[n]`; writes still succeed because acks=all only waits for the in-sync replicas. |
| `-Dsimplekafka.segment.bytes=1024` on a broker | Small segments, so you can watch `00000000000000000000.log`, `00000000000000000012.log`, ... appear. |
| `zkCli.sh -server localhost:2181` then `get /brokers/topics/test-topic/partitions/0/state` | The raw `leader=3;epoch=0;isr=3,1,2` the controller and leaders write. |

Broker settings you can override with `-D`: `simplekafka.data.dir`, `simplekafka.segment.bytes`,
`simplekafka.index.interval.bytes`, `simplekafka.replica.lag.time.ms`,
`simplekafka.zk.session.timeout.ms`, `simplekafka.unclean.leader.election`.

## How the code maps to the stages

### Stage 1: Project structure
`pom.xml` (Maven, Java 11, shade plugin for the single jar). Packages follow the layers:

```
com.simplekafka.protocol   wire format, requests/responses, KafkaConnection
com.simplekafka.zk         ZooKeeper client wrapper and path layout
com.simplekafka.storage    Log, LogSegment, OffsetIndex
com.simplekafka.broker     SimpleKafkaBroker, Controller, ReplicaManager, Partition, ReplicaFetcher
com.simplekafka.client     SimpleKafkaClient, SimpleKafkaProducer, SimpleKafkaConsumer
```

### Stage 2: Binary protocol (`protocol/`)
Every request is a size-prefixed frame: `size | apiKey | correlationId | clientId | body`;
responses are `size | correlationId | body`. The size lets the reader take exactly one
message off a TCP stream, and the correlation id pairs responses with requests. Four APIs:
PRODUCE, FETCH, METADATA and CREATE_TOPIC, each with a request and response class that
documents its byte layout. Error codes reuse real Kafka's numbers.

### Stage 3: ZooKeeper integration (`zk/`, `broker/SimpleKafkaBroker`)
The cluster state lives in ZooKeeper, in the same layout old Kafka used:

```
/brokers/ids/<id>                               ephemeral   live brokers
/controller                                     ephemeral   the controller's id
/brokers/topics/<topic>                         replica assignment, e.g. 0=1,2,3;1=2,3,1
/brokers/topics/<topic>/partitions/<p>/state    leader=1;epoch=0;isr=1,2,3
```

Ephemeral nodes disappear when a broker's session ends, which is how death is detected.
Watches are one-shot, so every handler re-reads with a new watch. All watch events are
handled one at a time on a single event thread, so state changes never race.

### Stage 4: Storage layer (`storage/`)
A partition's log is a directory of segments. Each segment is an append-only `.log`
file of records (`offset | size | crc | timestamp | key | value`) plus a sparse `.index`
mapping offsets to file positions, one entry per 4 KB. Reading an offset means a binary
search in the index and a short scan. On startup the segment re-validates CRCs after its
last index entry and cuts off a half-written tail. The high watermark is checkpointed to
`high-watermark-checkpoint`.

### Stage 5: The broker (`broker/`)
- **Controller**: whoever creates `/controller` first. It assigns replicas round-robin
  when a topic is created (written atomically with a ZooKeeper multi-op), and when a
  broker dies it removes it from every ISR and elects a new leader from the remaining
  in-sync replicas, bumping the leader epoch.
- **Leader and follower**: every broker watches partition state nodes. `ReplicaManager`
  makes the local `Partition` a leader or a follower. A follower truncates to its high
  watermark (dropping anything that might not be committed) and starts a `ReplicaFetcher`
  that pulls from the leader with ordinary FETCH requests.
- **ISR and high watermark**: the leader records each follower's fetch offset, sets the
  high watermark to the smallest log end offset in the ISR, adds a follower back once it
  catches up, and drops one that has not caught up for 10 seconds. ISR changes are
  compare-and-set writes to ZooKeeper, so the leader and controller never clobber each other.
- **Requests**: `SocketServer` is thread-per-connection. Non-controller brokers forward
  CREATE_TOPIC to the controller.

### Stage 6: Client library (`client/SimpleKafkaClient`)
Bootstraps from one broker, caches metadata (brokers, leaders), routes each produce or
fetch to the partition leader, and on NOT_LEADER, LEADER_NOT_AVAILABLE or a dropped
connection refreshes metadata and retries.

### Stage 7: Producer and consumer (`client/`)
The producer picks a partition by key hash (or round-robin without a key), creates the
topic if needed, and uses acks=all: a send returns only after every in-sync replica has
the message. The consumer reads one partition from a starting offset, long-polls the
leader, and only ever sees committed messages (below the high watermark).

## Simplifications compared with real Kafka

- Thread-per-connection blocking I/O instead of NIO selectors and request purgatories.
- One message per produce call from the CLI, no batching or compression.
- No consumer groups or committed consumer offsets; the consumer picks a partition and offset.
- No retention or log compaction, no topic deletion, no partition reassignment or preferred-leader rebalancing.
- Followers truncate to the high watermark rather than using leader-epoch checkpoints, which has the same known edge cases Kafka had before KIP-101.
- Partition state is plain text in ZooKeeper instead of JSON.

## Project layout

```
simple-kafka/
├── pom.xml
├── src/main/java/com/simplekafka/
│   ├── protocol/   wire format, requests and responses, KafkaConnection
│   ├── zk/         ZooKeeper client and path layout
│   ├── storage/    Log, LogSegment, OffsetIndex
│   ├── broker/     SimpleKafkaBroker, Controller, ReplicaManager, Partition, ReplicaFetcher
│   └── client/     SimpleKafkaClient, SimpleKafkaProducer, SimpleKafkaConsumer
├── src/test/java/  unit tests for the protocol and the log
└── docs/index.html browser playground (GitHub Pages)
```

## License

MIT, see [LICENSE](LICENSE).
