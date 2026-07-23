# Ezrah Credential Pipeline

An internal, asynchronous pipeline service that issues verifiable credentials. It has **no public HTTP interface**: upstream services talk to it over **gRPC**, work is sequenced through **Kafka**, and all state lives in **Postgres**. Built with **NestJS + TypeScript (strict)**.

A credential request flows through three sequential steps: **identity verification → claims validation → credential signing**, each of which can fail independently. The caller gets an immediate acknowledgement, can watch the request move through the pipeline in real time, and is notified via callback when it finishes or fails.

This README is the record of the engineering decisions. The [design decisions](#design-decisions-the-hard-parts) section is the part worth reading.

---

## Run it

**Prerequisite:** Docker (Desktop or Engine) running. Nothing else.

```bash
docker compose up --build
```

That single command:

1. starts **Postgres** and **Kafka** (KRaft mode, no Zookeeper),
2. runs a one-shot **`init`** service that applies the DB migration and provisions the Kafka topics with explicit partition counts, then exits,
3. starts the **app** (gRPC server + all Kafka consumers + the outbox relay + the reaper), which waits for `init` to finish so startup is race-free.

The gRPC server listens on `localhost:50051`.

### See it work

In a second terminal, with the stack up:

```bash
npm install       # once, for the demo client's deps
npm run demo
```

The demo submits a valid `KYCCredential` request, **streams the pipeline events to your console live**, receives the final outcome on a tiny local callback sink, then demonstrates duplicate submission (returns the same request id) and rejection of an invalid request. Because the steps have a configurable random failure rate (25% by default), you'll often see a step fail, retry with backoff, and recover. That's the point.

To see manual retry / resume-from-failure specifically:

```bash
npm run demo:retry
```

This submits a request with a blank required claim (passes ingest, fails the deterministic claims-validation step), calls the `Retry` RPC, and proves the already-succeeded identity step is **not** re-run; its attempt count is identical before and after the retry.

### Run the tests

```bash
npm test
```

Unit tests cover the domain rules, the event bus de-duplication, and, most importantly, the Watch replay→live-tail seam (the trickiest concurrency edge). They need no infrastructure.

---

## Design decisions (the hard parts)

### System architecture, and why

```
                         ┌──────────────────────────────────────────────┐
  upstream service       │                  APP (NestJS)                │
        │ gRPC           │                                              │
        ▼                │   Submit ─► IngestService ─┐                 │
  ┌───────────┐          │                            │ (1 tx)          │
  │  gRPC     │  Submit  │                            ▼                 │
  │  server   │─────────►│              ┌───────────────────────────┐   │
  │           │  Watch   │              │  Postgres (source of      │   │
  │           │◄─────────│◄── EventBus  │  truth)                   │   │
  └───────────┘  GetStatus│    ▲        │  • credential_request     │   │
        ▲         Retry   │    │        │  • step_execution         │   │
        │                 │    │        │  • pipeline_event (log)   │   │
        │ callback POST   │    │        │  • outbox_message         │   │
        │                 │    │        └───────────┬───────────────┘   │
  ┌───────────┐           │    │ live               │ poll (SKIP LOCKED)│
  │ callback  │◄──────────│ CallbackConsumer        ▼                   │
  │ sink      │           │    ▲            ┌────────────────┐          │
  └───────────┘           │    │            │  Outbox Relay  │          │
                          │  step consumers │  (DB ─► Kafka)  │         │
                          │  (identity /    └───────┬────────┘          │
                          │   claims /              │                   │
                          │   signing)   ◄──────────┼───────────┐       │
                          └──────────────│──────────│───────────│───────┘
                                         ▼          ▼           ▼
                            ┌─────────────────────────────────────────┐
                            │                 KAFKA                   │
                            │  identity.verify / claims.validate /    │
                            │  sign / events / callback / dlq         │
                            └─────────────────────────────────────────┘
```

Two ideas hold the whole thing together:

**1. Postgres is the source of truth; Kafka is the transport.** Every meaningful fact lives in Postgres: a request's status, each step's state, and the full ordered history of what happened. Kafka carries *work* between steps and *fans out* events. This split is deliberate: log-based transports are excellent at moving and sequencing work but are the wrong place to answer "what is the exact current state of request X?" A database is the right place for that.

**2. The transactional outbox.** The classic failure in a system like this is the *dual write*: you update the database, then publish to Kafka, and the process dies in between; now state and stream disagree. We never do two writes. Every state transition writes, **in one Postgres transaction**: (a) the new state, (b) the appended event(s), and (c) an `outbox_message` row describing the Kafka publish that should follow. A separate **outbox relay** drains that table to Kafka afterward. If we crash after the commit, the relay simply publishes on restart. State, history, and downstream work can never diverge.

Everything else (ordering, retries, duplicate-safety) falls out of these two ideas plus one more: an **atomic step claim**, below.

### Kafka topics and consumer groups

**One topic per pipeline step**, plus supporting topics:

| Topic | Partitions | Purpose |
| --- | --- | --- |
| `credential.identity.verify.v1` | 6 | step 1 command |
| `credential.claims.validate.v1` | 6 | step 2 command |
| `credential.sign.v1` | 6 | step 3 command |
| `credential.events.v1` | 6 | fan-out of every pipeline event |
| `credential.callback.v1` | 3 | terminal-outcome delivery |
| `credential.dlq.v1` | 3 | dead-letter park |

**One consumer group per responsibility:** `cg.identity-verifier`, `cg.claims-validator`, `cg.credential-signer`, `cg.callback-dispatcher`, `cg.dlq-monitor`, and a per-node `cg.events-fanout.<instanceId>`.

**What drove this:**

- **Ordering.** Every message is **keyed by `requestId`**. Kafka guarantees ordering within a partition, and a key always hashes to the same partition, so all of a request's messages are processed **in order by a single consumer**. That is what enforces "steps run in order", with no global lock or coordinator. Topic-per-step (rather than one topic with a `step` field) means each step gets its own consumer group, scales independently, and exposes its own consumer lag, which is the metric you actually want to alert on.
- **Fixed partition counts.** Command topics are keyed, so their partition count is effectively **immutable**: raising it live would remap `hash(requestId)` and let two consumers work the same request concurrently. The `.v1` suffix is the escape hatch: to change partitioning you create `.v2` and drain. (The atomic DB claim below is the backstop if a repartition ever slips through anyway.)
- **Events keyed by `requestId` too**, so a request's events stay ordered on one partition for any downstream consumer.
- **Fan-out vs. load-balancing.** The events-fanout group is unique *per node* precisely so every node receives the *full* event stream (to feed its local Watch streams). The step groups are shared across nodes so work is *load-balanced*. Same tool, opposite intent, chosen deliberately per topic.

### Pipeline event schema

One envelope, used for every event. `data` carries the type-specific payload so the envelope stays stable as payloads evolve.

```jsonc
{
  "eventId":    "uuid",
  "requestId":  "uuid",
  "sequence":   7,                    // monotonic per request; gaps => missed events
  "type":       "STEP_SUCCEEDED",     // REQUEST_ACCEPTED | STEP_STARTED | STEP_SUCCEEDED
                                      // | STEP_FAILED | REQUEST_COMPLETED | REQUEST_FAILED
  "step":       "CLAIMS_VALIDATION",  // or null for request-level events
  "status":     "SUCCEEDED",
  "occurredAt": "2026-07-22T10:00:00.000Z",
  "data":       { "attempt": 1 }      // credential, error+willRetry, failureStep+reason, ...
}
```

The design goal, straight from the brief, is that **a caller can reconstruct the full state of a request from the event stream alone.** Two properties make that true:

- **A per-request monotonic `sequence`.** It is allocated as `last_sequence + 1` under the request's row lock, inside the same transaction that appends the event. So it never has holes from rolled-back transactions, and a consumer can order strictly by it and *detect* a gap (a missed event) rather than silently misordering.
- **Events are append-only and complete.** Every transition emits its event(s) in the same transaction that makes it true. `REQUEST_COMPLETED` carries the signed credential; `REQUEST_FAILED` carries the failing step and reason. Replay the log in `sequence` order and you have re-derived the state machine.

### Real-time observation: how a caller watches progress

**Chosen approach: a gRPC server-streaming `Watch(requestId)`, backed by the append-only event log.**

When a caller opens `Watch`, the server **replays the event log from Postgres** (`sequence 0..N`) and then **tails live events**. So a subscriber that connects late still receives the complete history, and one that reconnects can resume from a `from_sequence`. The stream completes at the terminal event.

The subtle part is the **replay→live handoff**, which a naive implementation gets wrong by dropping events emitted between "read the log" and "subscribe to the live feed." We close that seam:

1. Subscribe to the live in-process bus **first** (buffering).
2. Read the log up to the current max.
3. Emit the replay, then flush the buffered live events, **de-duplicating strictly by `sequence`.**

Because an event is only pushed to the live bus *after* its row is committed, every event is guaranteed to be either in the log read or in the live buffer, never lost, and (thanks to sequence dedup) never delivered twice. This is unit-tested directly (`test/watch.service.spec.ts`) by injecting a live event into the middle of an in-flight log read.

The live bus is fed by two sources: the local commit (fast path, no Kafka round-trip for same-node watchers) and the per-node `events-fanout` consumer of `credential.events.v1` (so a watcher sees events produced on *other* nodes). The bus de-duplicates by `eventId`, so the overlap is harmless.

**Alternatives considered:**

- **Polling `GetStatus`.** Simplest, and it's included as a first-class RPC for pollers and health checks. But it isn't real-time and gets chatty under load. Kept as a complement, not the primary mechanism.
- **Let callers consume the Kafka events topic directly.** Maximally scalable and fully decoupled, but it leaks Kafka into every upstream caller and forces them to solve replay/ordering themselves. The events topic *exists* for services that genuinely want a firehose; the gRPC stream is the ergonomic default for "watch this one request."

### Duplicate submissions

Upstream may submit the same request more than once, possibly concurrently. Handling:

- Each request has an **`idempotencyKey`**: either caller-supplied, or derived as a content hash of `(subjectDid, credentialType, claims)` (order-stable, so key order doesn't matter). There is a **`UNIQUE` constraint** on it.
- Submit is a true **get-or-create**: the insert either wins (and *only the winner* emits `REQUEST_ACCEPTED` and queues step 1) or loses the unique-index race, in which case we return the **already-accepted request id**. A duplicate therefore never double-starts the pipeline, and the loser still gets a valid id back (not an error).
- Duplicate *delivery* of a step command (Kafka is at-least-once) is handled one layer down by the atomic claim, next.

### Retry and "resume from failure"

**"Resume from failure" means: on retry, re-drive only the first step that has not yet SUCCEEDED; steps that already succeeded keep their state and never run again.** Because each step's outcome is a durable `step_execution` row, this is just a query; there is no "replay from the beginning."

The mechanism, and why it's safe:

- **Atomic step claim.** Before doing any work, a consumer atomically claims the step: `UPDATE step_execution SET state='RUNNING', attempts=attempts+1 WHERE state IN ('PENDING','FAILED')`. Only one caller can win, even under a Kafka rebalance that redelivers the message to two consumers, because the request row is locked and the CAS only matches an unclaimed row. This is the single most important correctness mechanism: without it, a redelivery during signing could mint two credentials. The terminal transition is then optimistically guarded by the claimed attempt number, so a straggler can't double-advance the step either. (Signing is also **deterministic** over the request's stable inputs, so even a forced re-sign reproduces byte-identical output.)
- **Automatic retry** for *transient* failures (the simulated random failures in identity/signing). On failure with attempts remaining, the step is set back to `FAILED` and a re-drive is **scheduled via the outbox's `visible_after` column**: a delayed re-publish, not a sleep inside the consumer. (Sleeping in a consumer would trip Kafka's `max.poll.interval.ms` and trigger a rebalance; scheduling through the outbox avoids that entirely and needs no separate retry topic.)
- **Terminal failure** when attempts are exhausted (or immediately, for a *deterministic* claims-validation failure; bad data is a business outcome, not something to retry). The request is marked `FAILED` with the step and reason, the callback is queued, and exhausted-transient failures are also parked in the DLQ for inspection. Deterministic failures are *not* sent to the DLQ.
- **Manual retry** via the `Retry(requestId)` RPC: resets the first incomplete step to `PENDING` and re-queues it. For operator-driven recovery after a bug fix or a downstream outage.

### Resilience: the three questions the brief calls out

- **What happens when a step fails?** It halts *that* request only. A transient failure retries with backoff; a deterministic failure fails terminally. Either way, `STEP_FAILED` (and, if terminal, `REQUEST_FAILED`) is emitted with the reason, the callback fires, and enough state is persisted to understand and recover.
- **What happens when a consumer crashes mid-processing?** The step is left in `RUNNING`, which the atomic claim (correctly) refuses to re-claim, so a crash could otherwise strand it forever. The **reaper** is the answer: a periodic job re-drives any step that has been `RUNNING` longer than a lease (assumed-dead consumer), either retrying it or failing it if attempts are exhausted. Its transitions are optimistically guarded, so competing reapers or a revived original consumer can't double-act. (Kafka also redelivers the uncommitted message; the reaper is the proactive path, redelivery + claim the backstop.)
- **What happens when the same request arrives twice at the same time?** Two layers catch it: get-or-create at submit (unique idempotency key) and the atomic claim at each step. Concurrent duplicates converge on one request and one execution of each step.

### What would break first under high load, and how I'd address it

The **outbox relay** is the first thing I'd watch. It's the single conduit from DB to Kafka, and it polls. Under high write throughput, poll-based draining adds latency and the relay transaction (which publishes while holding `FOR UPDATE SKIP LOCKED` on a batch) becomes the bottleneck.

It is already built to **scale horizontally** (`SELECT ... FOR UPDATE SKIP LOCKED` means any number of relay instances cooperate with no double-publishing and no single point of failure), so the first move is simply to run more app replicas. Beyond that: shrink the poll interval, raise the batch size, partition the outbox by `aggregate_id` so relays shard cleanly, and ultimately replace polling with **logical decoding / CDC** (e.g. Debezium reading the WAL) to push outbox rows to Kafka with near-zero added latency.

The second pressure point is **Postgres write contention**: every transition takes the request's row lock. That's per-request, so it only hurts if a *single* request is extremely hot (unusual here); the global throughput ceiling is ordinary Postgres write capacity, addressed by connection pooling (PgBouncer) and, much later, sharding by `requestId`.

Slow **Watch** clients are the third: a stalled consumer must not back-pressure the shared bus. Each stream has a bounded buffer and is terminated with `RESOURCE_EXHAUSTED` on overflow (the client re-issues `Watch` and replays from the log, safe because the seam is closed).

### What I'd do differently / finish with more time

- **CDC instead of a polling relay** (above): the highest-value change for real load.
- **Split the deployable.** Today one process runs the gRPC server, every consumer, the relay, and the reaper: great for a single-command demo, wrong for production. Each consumer group and the relay should be independently scalable deployments; the code is already organized so this is a wiring change, not a rewrite.
- **A schema registry** (Avro/Protobuf) for the Kafka payloads, so event/command schemas are versioned and enforced across services rather than JSON-by-convention.
- **Richer retry policy**: jitter, per-error-class budgets, and a DLQ re-drive tool.
- **Integration tests with Testcontainers**: spin real Kafka + Postgres in CI and assert the end-to-end flows (crash-recovery, concurrent duplicates) that the unit tests can only approximate.
- **Observability**: OpenTelemetry traces threaded by `requestId`, plus per-topic lag and outbox-depth metrics.
- **Callback hardening**: signed payloads (HMAC), and per-destination circuit breaking.

---

## A note on process

Before writing any code, I ran an adversarial **multi-agent design red-team** against the architecture. It caught two genuinely load-bearing bugs in my first draft: a time-of-check/time-of-use idempotency guard that could double-mint a credential under a rebalance, and a lost-event seam in the Watch replay handoff, plus the poison-message and relay-HA concerns. All of them were folded in *before* implementation, which is why the atomic claim, the sequence-dedup seam, `SKIP LOCKED`, and the reaper are in the design rather than bolted on later. The critical fixes are the ones this README spends the most words on, and the seam fix is the one with a dedicated test.

---

## gRPC interface

See [`proto/credential_pipeline.proto`](proto/credential_pipeline.proto).

| RPC | Kind | Purpose |
| --- | --- | --- |
| `Submit` | unary | Accept a request; returns immediately with a `requestId`. Idempotent. |
| `Watch` | server-stream | Replay history + tail live events until terminal. |
| `GetStatus` | unary | Point-in-time snapshot (status + per-step state). |
| `Retry` | unary | Resume a `FAILED` request from its first incomplete step. |

### Credential types and required claims

| Credential type | Required claims |
| --- | --- |
| `EmploymentCredential` | `employerName`, `jobTitle`, `startDate` |
| `IdentityCredential` | `fullName`, `dateOfBirth`, `nationalId` |
| `KYCCredential` | `fullName`, `dateOfBirth`, `residenceCountry`, `documentType`, `documentNumber` |

Missing required keys are rejected at ingest (`INVALID_ARGUMENT`). Present-but-blank values pass ingest and are caught by the deterministic claims-validation step: a real, reproducible step-2 failure.

---

## Project layout

```
proto/credential_pipeline.proto   gRPC contract (the only public surface)
prisma/schema.prisma              DB schema: state machine, event log, outbox
prisma/migrations/                committed SQL migration
src/
  main.ts                         boots the gRPC microservice (no HTTP)
  config/env.config.ts            typed, validated environment
  common/topics.ts                Kafka topology (topics, groups, step order)
  domain/credential.ts            required claims, validation, deterministic signing
  prisma/                         Prisma client provider
  kafka/kafka.service.ts          idempotent producer + consumer factory + admin
  events/                         event envelope + in-process EventBus (dedup)
  pipeline/
    transition.service.ts         ALL state transitions (outbox, atomic claim, sequencing)
    ingest.service.ts             accept path (validation + get-or-create)
    watch.service.ts              Watch stream (replay -> live seam)
    outbox-relay.service.ts       drains outbox -> Kafka (SKIP LOCKED, HA)
    reaper.service.ts             recovers steps stranded by crashed consumers
    step-consumer.base.ts         shared claim -> work -> transition machinery
    steps/                        identity / claims / signing consumers
    callback.consumer.ts          idempotent outcome delivery
    dlq.consumer.ts               dead-letter monitor
    events-fanout.consumer.ts     per-node event fan-out for Watch
  grpc/pipeline.controller.ts     proto <-> service mapping
  scripts/provision-topics.ts     run-once topic provisioning (init container)
  scripts/demo-client.ts          end-to-end demo caller
  scripts/demo-retry.ts           manual-retry / resume-from-failure demo
test/                             unit tests (domain, event bus, Watch seam)
```

---

## Configuration

All via environment variables (see [`.env.example`](.env.example)); sensible defaults ship in `docker-compose.yml`. The knobs you'll actually touch:

| Variable | Default | Meaning |
| --- | --- | --- |
| `IDENTITY_FAILURE_RATE` / `SIGNING_FAILURE_RATE` | `0.25` | Simulated transient failure probability. Set to `0` for a deterministic happy-path demo. |
| `MAX_STEP_ATTEMPTS` | `4` | Attempts before a transient failure becomes terminal. |
| `STEP_TOPIC_PARTITIONS` | `6` | Partitions per keyed topic (per-request parallelism). |
| `OUTBOX_POLL_INTERVAL_MS` | `400` | Relay poll cadence (latency vs. DB load). |
| `OUTBOX_CLAIM_LEASE_MS` | `30000` | How long a relay's claimed batch is leased before another relay may re-claim it. |
| `STEP_LEASE_MS` | `60000` | How long a `RUNNING` step may live before the reaper re-drives it. |

---

## Known limitations / incomplete

Called out honestly, per the brief:

- **Single deployable.** One process runs everything (see *What I'd do differently*). Correct for the demo; you'd split it for production.
- **Polling outbox relay.** Works and scales horizontally, but CDC would be the production choice.
- **JSON on the wire for Kafka**, validated by convention rather than a schema registry.
- **The demo's callback sink** relies on `host.docker.internal` to reach the host from the container (works on Docker Desktop for Mac/Windows and, via the configured `host-gateway`, on Linux). If your setup can't route it, the Watch stream still demonstrates real-time observation fully; callback delivery is independently verified by the callback consumer's logic.
- **Integration tests** (real Kafka/Postgres via Testcontainers) are described but not included; unit tests cover the pure logic and the Watch seam.
