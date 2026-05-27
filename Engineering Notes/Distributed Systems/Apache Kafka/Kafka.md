# Apache Kafka

tags: #distributed-systems #messaging #backend #kafka

---

## What Is Kafka?

Apache Kafka is a **distributed event streaming platform** built for high-throughput, fault-tolerant, real-time data pipelines.

Originally built at **LinkedIn in 2011** to handle activity tracking at massive scale. Now used by Uber, Netflix, Airbnb, Twitter, and most large-scale distributed systems.

> Think of Kafka as a **durable, distributed commit log** that producers write to and consumers read from — at their own pace.

---

## The Problem Kafka Solves

### Without Kafka — Direct Service-to-Service Coupling

```
[Order Service] ──────→ [Notification Service]
              └────────→ [Inventory Service]
              └────────→ [Analytics Service]
              └────────→ [Fraud Detection]
```

Problems:
- Order Service needs to know about every downstream service
- If Notification Service is slow → Order Service blocks
- If Analytics crashes → messages are lost
- Adding a new service means changing the Order Service code

### With Kafka — Decoupled via Events

```
[Order Service] → [Kafka: order-events topic] → [Notification Service]
                                               → [Inventory Service]
                                               → [Analytics Service]
                                               → [Fraud Detection]
```

Benefits:
- Order Service just fires an event and moves on
- Each consumer reads independently at its own speed
- Add a new service without touching the producer
- Messages are **stored** — slow consumers catch up later

---

## The Traffic Spike Problem

Think of Uber or Zomato at peak hour:

- 10 million users × 1 location ping/sec = **10 million writes/sec**
- A database can handle maybe **50,000–100,000 writes/sec**
- Without a buffer → the DB is overwhelmed → latency spikes → system crashes

Kafka acts as that buffer:

```
[Millions of drivers] → [Kafka: accepts 10M/sec] → [DB: processes at safe pace]
```

Kafka ingests at extreme speed (disk-sequential writes), then drains into the DB at a rate it can handle.

---

## Architecture Overview

```
                    ┌──────────────────────────────────┐
                    │          KAFKA CLUSTER           │
                    │                                  │
[Producer 1] ──────►│  Broker 1   Broker 2   Broker 3  │──────► [Consumer Group A]
[Producer 2] ──────►│                                  │──────► [Consumer Group B]
[Producer 3] ──────►│  Topic: order-events             │
                    │  ├── Partition 0 (on Broker 1)   │
                    │  ├── Partition 1 (on Broker 2)   │
                    │  └── Partition 2 (on Broker 3)   │
                    │                                  │
                    │  ZooKeeper / KRaft (coordination)│
                    └──────────────────────────────────┘
```

---

## Core Concepts

### 🟠 Producer
- Sends messages to a Kafka **topic**
- Doesn't know or care who reads the message
- Can specify a **partition key** to control which partition a message goes to
- Configurable delivery guarantees: fire-and-forget, ack from leader, ack from all replicas

### 🟡 Broker
- A single **Kafka server** — stores messages on disk
- A **cluster** = multiple brokers (typically 3+)
- Each broker holds some partitions from each topic
- One broker per partition acts as the **leader** — handles reads and writes
- Others are **replicas** — take over if the leader fails

### 🔵 Topic
- A **named stream** of records (like a table in a DB, but append-only)
- Producers write to a topic, consumers subscribe to a topic
- Topics are **durable** — messages stay until a retention period expires (default: 7 days)
- Examples: `order-events`, `driver-location`, `payment-completed`

### 🟢 Partition
- A topic is split into **N partitions** for parallelism
- Each partition is an **ordered, immutable sequence** of messages
- More partitions = more parallel consumers = higher throughput
- Messages within a partition are assigned a monotonically increasing **offset**

### 🔴 Consumer
- Reads messages from a topic
- Tracks which messages it has read via **offsets**
- Can replay from any offset — Kafka doesn't delete on read
- One partition is assigned to **one consumer at a time** (within a group)

### 🟣 Consumer Group
- A named group of consumers that **collectively read a topic**
- Kafka distributes partitions across the consumers in the group
- Two different groups both receive **all messages independently**
- Enables both **queue** semantics (within group) and **pub/sub** (across groups)

### 📦 Offset
- A sequential integer ID for each message within a partition
- The consumer decides when to **commit** its offset (mark message as processed)
- Enables replay, fault tolerance, and exactly-once processing patterns

---

## Kafka's Hybrid Messaging Model

| Pattern | Behavior | Kafka equivalent |
|---|---|---|
| **Queue** | One message → processed by one consumer | Within a consumer group |
| **Pub/Sub** | One message → all subscribers receive it | Across different consumer groups |

This makes Kafka unusually flexible — one topic can serve both patterns simultaneously.

---

## Key Guarantees

| Guarantee | Detail |
|---|---|
| **Ordering** | Messages within a partition are strictly ordered |
| **Durability** | Messages written to disk, replicated across brokers |
| **Fault tolerance** | If a broker dies, replicas take over (no data loss) |
| **Replay** | Consumers can re-read from any past offset |
| **Scalability** | Add partitions/brokers horizontally |

---

## Kafka vs Traditional Message Queues (RabbitMQ, SQS)

| Feature | Kafka | RabbitMQ / SQS |
|---|---|---|
| Message retention | Stored for days/weeks | Deleted after consumption |
| Replay | Yes | No |
| Ordering | Per-partition | Limited |
| Throughput | Very high (millions/sec) | Moderate |
| Consumer model | Pull | Push (RabbitMQ) or Pull (SQS) |
| Best for | Event streams, logs, pipelines | Task queues, job dispatch |

---

## Related Notes

- [[Kafka - Consumer Groups]]
- [[Kafka - Partitions & Offsets]]
- [[Kafka - Use Cases]]
- [[Message Queues]]
- [[System Design - Uber]]
- [[Microservices]]
