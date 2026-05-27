# Kafka — Partitions & Offsets

tags: #kafka #distributed-systems #partitions #offsets #ordering

---

## What Is a Partition?

A **partition** is the fundamental unit of storage and parallelism in Kafka.

Every topic is split into one or more partitions. Each partition is:
- An **ordered, immutable, append-only log** of messages
- Stored on a specific broker's disk
- Replicated to other brokers for fault tolerance

```
Topic: order-events  (3 partitions)

Partition 0:  [msg 0] → [msg 1] → [msg 2] → [msg 5] → [msg 8]
Partition 1:  [msg 0] → [msg 1] → [msg 3] → [msg 6]
Partition 2:  [msg 0] → [msg 2] → [msg 4] → [msg 7] → [msg 9]
```

Offsets restart from 0 **within each partition** — they are not global across the topic.

---

## Why Partitions Exist

### 1. Parallelism (Scalability)
Without partitions, only one consumer could read a topic at a time. With N partitions, N consumers can read in parallel.

```
No partitions:        3 partitions:
[Topic] → [C1]        P0 → [C1]
                      P1 → [C2]
                      P2 → [C3]
```

Throughput scales linearly with partitions (up to hardware limits).

### 2. Horizontal Scaling Across Brokers
Partitions are distributed across brokers in the cluster. A 12-partition topic across a 3-broker cluster:

```
Broker 1: P0, P1, P2, P3  (leaders)
Broker 2: P4, P5, P6, P7  (leaders)
Broker 3: P8, P9, P10, P11 (leaders)
```

Each broker handles only its share of writes and reads.

### 3. Fault Tolerance via Replication
Each partition is replicated to `replication.factor` brokers. One is the **leader** (handles reads/writes), others are **followers** (replicate data).

```
Partition 0:
  Leader  → Broker 1  ← producers write here, consumers read here
  Replica → Broker 2  ← replicates from leader
  Replica → Broker 3  ← replicates from leader

If Broker 1 dies → Broker 2 or 3 becomes the new leader automatically
```

---

## How Partitioning Works (Producer Side)

When a producer sends a message, Kafka decides which partition it goes to:

### No key specified → Round Robin
```
msg 1 → P0
msg 2 → P1
msg 3 → P2
msg 4 → P0  (cycles back)
```
Good for even distribution. No ordering guarantee across partitions.

### Key specified → Hash-based routing
```python
partition = hash(key) % num_partitions
```

All messages with the **same key always go to the same partition**.

```
key="user-123" → always → Partition 1
key="user-456" → always → Partition 2
key="user-789" → always → Partition 0
```

This guarantees **ordering for all events of the same entity**.

### Custom partitioner
You can write your own logic — e.g., route VIP users to a dedicated partition for lower latency.

---

## Partition Keys — Design Decisions

Choosing the right partition key is critical for correctness and performance.

| Key choice | Result | Risk |
|---|---|---|
| `user_id` | All events for a user are ordered | Skew if some users generate far more events |
| `order_id` | Order lifecycle events are ordered | Usually fine — uniform distribution |
| `region` | Events grouped by geography | Hotspot if one region dominates |
| No key | Max distribution | No ordering across messages |

### The Hotspot Problem
If one key generates far more traffic than others, one partition gets overloaded while others idle.

```
key="US-EAST" → 90% of traffic → Partition 0 is a bottleneck
key="EU-WEST" → 5% of traffic → Partition 1 is underused
key="APAC"   → 5% of traffic → Partition 2 is underused
```

Fix: Use a more granular key (`region + user_id`), or use custom partitioner logic to redistribute hot keys.

---

## What Is an Offset?

An **offset** is a sequential integer that uniquely identifies a message **within a partition**.

```
Partition 0:
  offset 0: {"event": "order_placed", "id": "A1"}
  offset 1: {"event": "payment_received", "id": "A1"}
  offset 2: {"event": "order_shipped", "id": "A1"}
  offset 3: {"event": "order_placed", "id": "B7"}
  offset 4: {"event": "order_placed", "id": "C2"}
         ↑
    Current latest offset = 4
```

Properties of offsets:
- Monotonically increasing (never resets while partition exists)
- Unique only within a partition (not globally unique across a topic)
- Immutable — an offset always points to the same message forever
- Kafka does not delete messages on read — the message stays at its offset

---

## How Consumers Use Offsets

Every consumer group tracks its **current offset per partition** — this is stored in the internal `__consumer_offsets` topic.

```
Consumer Group "analytics" state:
  topic=order-events, partition=0, offset=47  (next message to read = 47)
  topic=order-events, partition=1, offset=31
  topic=order-events, partition=2, offset=58
```

On restart, the consumer fetches these offsets and resumes from where it left off — no messages are lost or reprocessed (assuming proper commit behavior).

---

## Offset Commit Strategies

### commitSync()
Blocks until the broker acknowledges the offset commit. Safe but slower.

```
for message in poll():
    process(message)
    consumer.commitSync()   ← waits for broker ack
```

### commitAsync()
Non-blocking. Faster but if the commit fails, it won't retry automatically.

```
for message in poll():
    process(message)
    consumer.commitAsync()  ← fire-and-forget offset commit
```

### Batch commits
Commit only every N messages or every T seconds to reduce commit overhead.

---

## Replaying Messages

Because offsets are persistent and messages aren't deleted immediately, you can **seek to any past offset** and replay.

```python
consumer.seek(partition=TopicPartition("order-events", 0), offset=0)
```

Use cases for replay:
- A downstream service had a bug and processed data incorrectly — replay to reprocess correctly
- A new analytics service joins late — replay all historical events to build its initial state
- Debugging — re-examine exactly what happened at a specific time

### Retention Policy
Kafka retains messages until:
- Time-based: `log.retention.hours` (default: 168 = 7 days)
- Size-based: `log.retention.bytes` (max size per partition before old segments are deleted)
- Compaction: For compacted topics, only the **latest message per key** is retained (useful for state/snapshots)

---

## Log Compaction

A special topic config (`cleanup.policy=compact`) where Kafka keeps only the **most recent value per key**.

```
Before compaction:
  offset 0: key="user-1" value="name=Alice"
  offset 1: key="user-2" value="name=Bob"
  offset 2: key="user-1" value="name=Alice Smith"   ← update
  offset 3: key="user-1" value="name=Alicia Smith"  ← update

After compaction:
  offset 2: key="user-1" value="name=Alice Smith"   ← older overwrite kept during active window
  offset 3: key="user-1" value="name=Alicia Smith"  ← latest retained
  offset 1: key="user-2" value="name=Bob"
```

Use case: maintaining the **latest state** of an entity (like a changelog) without growing forever.

---

## How Many Partitions Should You Use?

No universal answer — depends on your system, but a starting framework:

```
target_throughput = MB/sec you need to produce or consume
partition_count   = target_throughput / throughput_per_partition
```

Typical throughput per partition ≈ 10–50 MB/sec (depends on hardware, message size, replication).

**Rules of thumb:**
- Start with more partitions than you think you need — reducing partitions later requires recreating the topic
- Common starting points: 6, 12, 24 (multiples of your broker count and consumer count)
- Each partition consumes memory on brokers and clients — don't go wild (10,000+ partitions per cluster causes overhead)

---

## Summary

| Concept | One-liner |
|---|---|
| Partition | Ordered, append-only log; unit of parallelism and storage |
| Partition key | Determines which partition a message lands on |
| Offset | Sequential ID for a message within a partition |
| Offset commit | Consumer marking a message as processed |
| Replay | Seeking to a past offset to re-consume messages |
| Replication | Each partition copied to N brokers for fault tolerance |
| Log compaction | Retaining only the latest value per key in a partition |

---

## Related Notes

- [[Kafka]]
- [[Kafka - Consumer Groups]]
- [[Kafka - Use Cases]]
