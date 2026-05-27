# Kafka — Consumer Groups

tags: #kafka #distributed-systems #consumers #parallelism

---

## What Is a Consumer Group?

A **consumer group** is a named set of consumers that work together to read a Kafka topic.

Kafka distributes the topic's **partitions** across the consumers in the group, so each partition is consumed by exactly one consumer at a time — enabling parallel processing without duplicate reads.

```
Topic: order-events  (4 partitions)

Consumer Group: "notifications-service"

  Partition 0 ──► Consumer A
  Partition 1 ──► Consumer A
  Partition 2 ──► Consumer B
  Partition 3 ──► Consumer B
```

> Each message is processed by **one consumer per group** — but every group gets all messages independently.

---

## The Two Messaging Models in One

Kafka achieves both Queue and Pub/Sub using consumer groups:

### Queue (within a group)
One message → processed by exactly one consumer in the group

```
Topic: payments
Group: fraud-detection

  [msg 1] → Consumer 1
  [msg 2] → Consumer 2   ← different consumer handles it
  [msg 3] → Consumer 1
```

No duplicate work. Natural load balancing.

### Pub/Sub (across groups)
One message → received by every consumer group independently

```
Topic: order-events

  Group: notification-service  → receives ALL messages
  Group: analytics-service     → receives ALL messages (independently)
  Group: inventory-service     → receives ALL messages (independently)
```

Adding a new service = just create a new consumer group. Zero changes to the producer.

---

## Partition-to-Consumer Assignment Rules

| Scenario | Result |
|---|---|
| consumers < partitions | Some consumers handle multiple partitions |
| consumers = partitions | Perfect 1:1 assignment (ideal) |
| consumers > partitions | Excess consumers sit idle |

```
4 partitions, 2 consumers:           4 partitions, 4 consumers:
  P0, P1 → Consumer 1                  P0 → Consumer 1
  P2, P3 → Consumer 2                  P1 → Consumer 2
                                        P2 → Consumer 3
4 partitions, 6 consumers:             P3 → Consumer 4
  P0 → Consumer 1
  P1 → Consumer 2                   4 partitions, 2 consumers (heavy):
  P2 → Consumer 3                     One consumer gets 2 partitions
  P3 → Consumer 4                     (can be a hotspot if partitions unequal)
  Consumer 5 → idle
  Consumer 6 → idle
```

**Key insight:** The number of partitions sets the **maximum parallelism** for a consumer group. To scale beyond N consumers, you must first add more partitions.

---

## Rebalancing

A **rebalance** occurs when the group membership changes:
- A new consumer joins the group
- A consumer crashes or shuts down
- Partitions are added to the topic

During a rebalance, Kafka **pauses all consumption** and reassigns partitions across the current consumers.

```
Before rebalance (3 consumers, 6 partitions):
  C1: P0, P1
  C2: P2, P3
  C3: P4, P5

Consumer C2 crashes → rebalance triggered:
  C1: P0, P1, P2
  C3: P3, P4, P5
```

### Why Rebalances Are Expensive
- All consumers in the group stop processing during rebalance
- The group coordinator (a Kafka broker) runs a negotiation protocol
- For large groups with many partitions, this can take seconds

### How to Reduce Rebalance Frequency
- Use **static group membership** (`group.instance.id`) — Kafka remembers a consumer's assigned partitions so a temporary disconnect doesn't trigger a full rebalance
- Tune `session.timeout.ms` and `heartbeat.interval.ms` — avoid false timeouts on slow consumers
- Use **cooperative/incremental rebalancing** (Kafka 2.4+) — only revoked partitions are moved, not all

---

## Consumer Lag

**Consumer lag** = how far behind a consumer group is from the latest messages in a topic.

```
Topic partition: [msg 0] [msg 1] [msg 2] [msg 3] [msg 4] [msg 5]
                                                              ↑
                                                        Latest offset = 5

Consumer last committed offset = 2
                  ↑
              Consumer lag = 5 - 2 = 3
```

High consumer lag means:
- The consumer is too slow for the production rate
- A consumer crashed and isn't processing
- Processing logic has become a bottleneck

### Monitoring Lag
Use `kafka-consumer-groups.sh --describe` or tools like **Kafka UI**, **Burrow**, **Confluent Control Center**, or **Prometheus + Grafana**.

---

## Offset Management

Each consumer group tracks its own offsets (per partition) in a special Kafka topic: `__consumer_offsets`.

### Auto Commit (default)
Kafka automatically commits the offset every `auto.commit.interval.ms` (default 5s).

Risk: If the consumer processes a message but crashes before the auto-commit → the message is **reprocessed** on restart (at-least-once delivery).

### Manual Commit
The consumer explicitly calls `commitSync()` or `commitAsync()` after processing.

```
poll messages
→ process message
→ commitSync()   ← only commits after confirmed processing
```

More control but requires careful implementation.

### At-Least-Once vs At-Most-Once vs Exactly-Once

| Semantic | How | Trade-off |
|---|---|---|
| **At-most-once** | Commit before processing | Fast, but can lose messages on crash |
| **At-least-once** | Commit after processing | Safe, but can duplicate on crash |
| **Exactly-once** | Transactional API | Strongest guarantee, higher complexity |

Most real-world systems use **at-least-once** and make processing **idempotent** (safe to run twice).

---

## Group Coordinator

Each consumer group has a **group coordinator** — a Kafka broker responsible for:
- Tracking which consumers are alive (via heartbeats)
- Triggering rebalances when membership changes
- Storing committed offsets in `__consumer_offsets`

The coordinator is determined by hashing the `group.id` to a partition in `__consumer_offsets`, and whichever broker leads that partition becomes the coordinator.

---

## Practical Design Tips

- **Scale consumers = scale partitions.** If you need 10 parallel consumers, ensure the topic has at least 10 partitions.
- **Name your groups meaningfully.** `analytics-service` is better than `group1` — it shows up in monitoring.
- **Each independent service = its own consumer group.** Never share a group across different services.
- **Idempotent consumers** are your safety net. Design processing so replaying a message doesn't cause double-charges, duplicate emails, etc.
- **Monitor lag continuously.** Sudden lag spikes signal processing bottlenecks or consumer failures.

---

## Related Notes

- [[Kafka]]
- [[Kafka - Partitions & Offsets]]
- [[Kafka - Use Cases]]
