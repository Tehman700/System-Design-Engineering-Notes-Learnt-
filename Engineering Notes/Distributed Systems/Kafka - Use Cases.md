# Kafka — Use Cases

tags: #kafka #distributed-systems #system-design #use-cases

---

## When to Reach for Kafka

Kafka shines when you need:
- **High throughput** ingestion that outpaces what a DB can absorb directly
- **Decoupled services** that shouldn't know about each other
- **Message replay** — the ability to re-read historical events
- **Fan-out** — one event consumed by multiple independent services
- **Real-time pipelines** — data flowing from source to destination continuously

It's overkill for simple task queues, low-volume notifications, or simple job dispatch (use RabbitMQ/SQS there).

---

## Use Case 1: Real-Time Location Tracking (Uber / Lyft)

**Problem:** Millions of drivers send GPS pings every second. You need to:
- Show riders the driver's live location
- Detect ETAs and route changes
- Feed data to surge pricing algorithms
- Store historical routes for fraud detection

**Without Kafka:** Every ping hits the DB directly → DB can't handle 10M writes/sec → system crashes.

**With Kafka:**
```
[Driver App]
     ↓ (GPS ping every 1 sec)
[Kafka: driver-location topic]
     ↓               ↓              ↓
[Live Map Service] [Surge Engine] [Route History DB]
```

- Kafka buffers all location updates
- Live map reads latest offset (near-real-time)
- Surge engine does windowed aggregations
- Route history service writes to cold storage at its own pace

**Key design choice:** Partition by `driver_id` so all pings from one driver arrive in order.

---

## Use Case 2: Order Processing Pipeline (E-commerce)

**Problem:** When a user places an order, you need to:
- Deduct inventory
- Send a confirmation email
- Notify the warehouse
- Trigger fraud checks
- Update analytics dashboards

Doing all this synchronously in one request = fragile, slow, hard to change.

**With Kafka:**
```
[Order Service] → Kafka: order-placed →  [Inventory Service]
                                      →  [Email Service]
                                      →  [Warehouse Service]
                                      →  [Fraud Detection]
                                      →  [Analytics]
```

Each downstream service is its own consumer group. Adding a new step (e.g. loyalty points) = add one consumer group, zero changes to Order Service.

**Replay benefit:** If the Fraud Detection service had a bug for 2 hours, you can replay those 2 hours of events after the fix is deployed — no events are lost.

---

## Use Case 3: Log Aggregation (Netflix / LinkedIn)

**Problem:** You have hundreds of microservices, each writing logs. You need centralized search, alerting, and archival.

**Without Kafka:** Each service writes directly to Elasticsearch or S3 → tight coupling, Elasticsearch can't absorb burst traffic.

**With Kafka:**
```
[Service A logs]  ─┐
[Service B logs]  ─┤→ [Kafka: app-logs] → [Elasticsearch (search)]
[Service C logs]  ─┤                    → [S3 (archival)]
[Service D logs]  ─┘                    → [Alerting Engine]
```

Benefits:
- Services just fire-and-forget log events
- Elasticsearch ingests at a sustainable rate
- If Elasticsearch goes down, logs buffer in Kafka and drain when it recovers
- Multiple consumers (search, archive, alert) each see every log

This is literally the original LinkedIn use case that led to Kafka being built.

---

## Use Case 4: Event Sourcing

**Problem:** Instead of storing just the current state of a record, you want to store every change that ever happened to it — so you can reconstruct state at any point in time.

**Kafka as the event store:**
```
Kafka topic: account-events (compacted)

  offset 0: {userId: 1, event: "account_created", balance: 0}
  offset 1: {userId: 1, event: "deposit", amount: 500, balance: 500}
  offset 2: {userId: 1, event: "purchase", amount: 120, balance: 380}
  offset 3: {userId: 1, event: "deposit", amount: 1000, balance: 1380}
```

Replay from offset 0 → rebuild current state at any time.
Replay to offset 2 → know the state at that exact moment.

Used by: financial systems, audit trails, CQRS architectures, collaborative editing tools.

---

## Use Case 5: Stream Processing (Fraud Detection)

**Problem:** Detect fraudulent transactions in real-time — before the transaction completes.

**With Kafka + Flink/Kafka Streams:**
```
[Payments topic] → [Kafka Streams App]
                        ↓
               Sliding window: "Has this card made
               5+ transactions in the last 60 seconds
               from different countries?"
                        ↓
               [fraud-alerts topic] → [Block transaction API]
```

Kafka Streams / Apache Flink processes messages as they arrive, maintaining in-memory state (e.g., per-user windows) to detect patterns.

**Other stream processing patterns:**
- Joining two streams (e.g., correlate orders with payments)
- Aggregations over time windows (clicks per minute, transactions per hour)
- Enrichment (look up user profile data and attach it to the event)

---

## Use Case 6: Database Change Data Capture (CDC)

**Problem:** You want other services to react to DB changes without polling the DB constantly.

**With Kafka + Debezium:**
```
[PostgreSQL] → [Debezium connector] → [Kafka: db-changes topic]
                                            ↓            ↓
                                    [Search Index]  [Cache Invalidation]
                                    (Elasticsearch) (Redis)
```

Debezium reads the DB's **write-ahead log (WAL)** and emits every insert/update/delete as a Kafka message. Downstream services react to these changes in real-time without touching the DB.

Use cases: keeping Elasticsearch in sync with Postgres, invalidating Redis caches, syncing microservice databases.

---

## Use Case 7: Metrics & Monitoring Pipeline

**Problem:** Collect system metrics (CPU, memory, request latency) from thousands of servers and make them queryable in real-time.

```
[Server 1 metrics]  ─┐
[Server 2 metrics]  ─┤→ [Kafka: metrics] → [Prometheus / InfluxDB]
[Server N metrics]  ─┘                   → [Grafana dashboards]
                                          → [Alerting (PagerDuty)]
```

Benefits:
- Metrics are durable — if Grafana goes down, no metrics are lost
- Multiple consumers: dashboards, alerts, long-term storage all get the same stream
- Kafka can handle millions of metric data points per second

---

## Use Case 8: Notification Fan-Out (Social Media)

**Problem:** User posts something. 10,000 followers need to be notified. You can't do this synchronously.

```
[Post created event]
        ↓
[Kafka: post-events]
        ↓
[Fan-out Worker: reads followers from DB, writes to notification queue per user]
        ↓
[Push Notification Service]  [Email Digest Service]  [In-App Feed Service]
```

The fan-out worker reads one Kafka event and creates thousands of individual notification tasks. Each downstream service picks up the relevant tasks independently.

---

## Anti-Patterns: When NOT to Use Kafka

| Situation | Why Kafka is wrong | Use instead |
|---|---|---|
| Simple background jobs (resize image, send email) | Overkill — no need for pub/sub or replay | Celery, BullMQ, SQS |
| Request/response RPC | Kafka is async and one-directional | gRPC, REST |
| Very low volume (< 1000 msgs/day) | Operational overhead not worth it | Redis Pub/Sub, SQS |
| You need message priorities | Kafka is strictly ordered per partition | RabbitMQ (supports priority queues) |
| Tiny team, early startup | Too much infra to manage | Managed queue (SQS, Cloud Pub/Sub) |

---

## Real Companies Using Kafka

| Company | How they use it |
|---|---|
| **LinkedIn** | Activity feeds, metrics, operational monitoring (invented Kafka) |
| **Uber** | Driver location tracking, surge pricing, ride matching |
| **Netflix** | Event processing pipeline, real-time recommendations, A/B test tracking |
| **Airbnb** | Data pipelines, analytics, microservice communication |
| **Twitter** | Activity streams, real-time analytics, ad targeting |
| **Confluent** | Managed Kafka-as-a-service (built by Kafka's original creators) |

---

## Decision Framework: Do You Need Kafka?

```
High throughput (>10K msgs/sec)?              → Yes → strong signal for Kafka
Multiple services need same events?           → Yes → strong signal for Kafka
Need to replay past events?                   → Yes → Kafka (queues can't do this)
Events need to be processed in order?         → Yes → Kafka (per partition)
Simple job queue (one worker handles task)?   → No  → SQS / RabbitMQ is simpler
Synchronous request-response?                 → No  → REST or gRPC
```

---

## Related Notes

- [[Kafka]]
- [[Kafka - Consumer Groups]]
- [[Kafka - Partitions & Offsets]]
- [[Message Queues]]
- [[System Design - Uber]]
- [[Stream Processing]]
