# Apache Kafka

tags: #distributed-systems #messaging #backend #kafka

---

## The Problem Kafka Solves

Think of Uber or Zomato at peak time.
Every driver/user sends a request **every second**.

- 10 million users × 1 req/sec = **10 million writes/sec to DB**
- Databases have **high throughput but can't handle that speed**
- Result → latency spikes, system slows down or crashes

> The DB becomes the bottleneck. You need something that sits in front of it.

---

## What Kafka Does

Kafka sits **between your services and your database** as a buffer.

- Lower throughput than a DB but **much faster at accepting incoming data**
- Says: *"Give me everything, I'll hold it, and feed it downstream at a safe pace"*
- Think of it like a **highway flyover** — traffic doesn't stop, it just flows through

---

## Core Concepts

### 🟠 Producer
- The one **sending messages** (e.g. driver location update, order placed)
- Doesn't care who reads it — just fires and forgets

### 🟡 Broker
- The **Kafka server** — stores and manages messages
- A Kafka cluster = multiple brokers for reliability

### 🔵 Topic
- A **named channel** for a category of messages
- e.g. `driver-location`, `order-events`, `payments`
- Producer writes to a topic, consumer reads from a topic

### 🟢 Partition
- A topic is split into **partitions** for parallelism
- Messages inside a partition are **ordered**
- More partitions = more consumers can read simultaneously

### 🔴 Consumer
- The one **reading messages** from a topic
- One partition → assigned to **one consumer only** at a time
- This avoids duplicate processing

### 🟣 Consumer Group
- A **group of consumers** working together to read a topic
- Each consumer in the group gets **different partitions**
- Two different groups = both get **all messages independently**
- e.g. Group A = notification service, Group B = analytics service
  → both read `order-events` separately, neither blocks the other

---

## The Model Kafka Uses

Kafka is a hybrid of two patterns:

| Pattern | What it means |
|---|---|
| **Queue** | One message → processed by one consumer only |
| **Pub/Sub** | One message → broadcast to multiple subscriber groups |

Within a consumer group → **Queue**
Across different consumer groups → **Pub/Sub**

---

## Simple Mental Model

```
[Driver App]  →  Producer
                    ↓
              [ Kafka Broker ]
              Topic: driver-location
              Partition 0 | Partition 1 | Partition 2
                    ↓             ↓             ↓
              Consumer A    Consumer B    Consumer C
              (same group → each gets a different partition)

              Consumer Group X → Notifications Service
              Consumer Group Y → Analytics Service
              (both read the same topic independently)
```

---

## When to Use Kafka

- High volume event streams (location pings, clicks, orders)
- Decoupling microservices from each other
- When you need **message replay** — Kafka stores messages, you can re-read old ones
- Real-time data pipelines (feed into Spark, Flink, a DB, etc.)

---

## Related Notes

- [[Message Queues]]
- [[System Design - Uber]]
- [[Microservices]]
- [[Redis vs Kafka]]
