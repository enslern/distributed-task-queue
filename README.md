# distributed-task-queue

Built this to understand how task queues like Celery actually work under the hood — not by reading docs, but by building one from scratch.

I'm an EEE student at NIT Goa who got into systems programming. Most projects I saw online were either too simple or just wrappers around existing tools. I wanted to know what happens at the socket level, how workers coordinate, what makes a system fault-tolerant. So I built it.

No Celery. No Redis. No shortcuts.

![Dashboard](assets/dashboard.png)

---

## What it does

You submit a task. It goes into a priority queue on the broker. A worker picks it up, executes it in a separate process, and sends the result back.

If the worker dies mid-task, the broker detects it via heartbeat timeout and requeues the task automatically. If a task fails, it retries with exponential backoff. If it keeps failing after max retries, it goes into a dead letter queue. Everything is persisted to SQLite so a broker restart doesn't lose tasks.

---

## Architecture

### System overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                             │
│                                                                 │
│   client.submit_task("add", 2, 3)   @task decorator            │
│   AsyncResult.wait_for_result()     result.get_status()        │
└───────────────────────────┬─────────────────────────────────────┘
                            │ TCP (length-prefixed JSON)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                        BROKER (asyncio)                         │
│                                                                 │
│   ┌─────────────┐   ┌──────────────┐   ┌────────────────────┐  │
│   │ Priority    │   │  Heartbeat   │   │  Worker Registry   │  │
│   │ Queue       │──▶│  Monitor     │──▶│  (active workers)  │  │
│   │             │   │  (10s TTL)   │   │                    │  │
│   └─────────────┘   └──────────────┘   └────────────────────┘  │
│          │                                        │             │
│          └──────────────────┬─────────────────────┘            │
│                             │                                   │
│                    ┌────────▼────────┐                          │
│                    │   SQLite DB     │                          │
│                    │  tasks/results  │                          │
│                    │  workers        │                          │
│                    └─────────────────┘                          │
└───────────────────────────┬─────────────────────────────────────┘
                            │ TCP (length-prefixed JSON)
          ┌─────────────────┼──────────────────┐
          ▼                 ▼                  ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│   WORKER 1   │   │   WORKER 2   │   │   WORKER N   │
│              │   │              │   │              │
│ ProcessPool  │   │ ProcessPool  │   │ ProcessPool  │
│ Executor     │   │ Executor     │   │ Executor     │
│ (4 procs)    │   │ (4 procs)    │   │ (4 procs)    │
│              │   │              │   │              │
│ heartbeat    │   │ heartbeat    │   │ heartbeat    │
│ every 3s     │   │ every 3s     │   │ every 3s     │
└──────────────┘   └──────────────┘   └──────────────┘
```

### Message flow — task lifecycle

```
Client                   Broker                    Worker
  │                        │                          │
  │── SUBMIT_TASK ────────▶│                          │
  │                        │ insert to DB (pending)   │
  │                        │ push to priority queue   │
  │◀─ task_id ─────────────│                          │
  │                        │                          │
  │                        │◀─ REQUEST_TASK ──────────│
  │                        │── TASK_ASSIGNED ────────▶│
  │                        │ update DB (running)      │
  │                        │                          │ execute in
  │                        │                          │ ProcessPool
  │                        │                          │
  │                        │◀─ TASK_RESULT ───────────│
  │                        │ save to results table    │
  │                        │ update DB (success)      │
  │                        │                          │
  │── GET_RESULT ─────────▶│                          │
  │◀─ result ──────────────│                          │
```

### Failure & retry flow

```
Worker                   Broker                    New Worker
  │                        │                          │
  │ task fails             │                          │
  │ retry_count += 1       │                          │
  │── SUBMIT_TASK ────────▶│ (new task_id,            │
  │   (retry_count=N)      │  retry_count=N)          │
  │                        │ push back to queue       │
  │                        │                          │
  │ [if retry_count        │                          │
  │  >= MAX_RETRIES]       │                          │
  │── TASK_RESULT ────────▶│ status = failed          │
  │   (status=failed)      │ (dead letter queue)      │
  │                        │                          │
  │ [if worker crashes]    │                          │
  ✗ (no heartbeat)         │ 10s timeout              │
                           │ requeue task             │
                           │── TASK_ASSIGNED ────────▶│
```

### Broker crash recovery

```
Broker restart
     │
     ▼
Query SQLite:
SELECT * FROM tasks
WHERE status IN ('pending', 'running')
     │
     ▼
Push all back into priority queue
     │
     ▼
Resume normal operation
(zero task loss)
```

### TCP protocol — message framing

```
┌────────────────┬──────────────────────────────────────┐
│   4 bytes      │           N bytes                    │
│  (big-endian)  │         (JSON payload)               │
│   length = N   │  {"type": "SUBMIT_TASK", ...}        │
└────────────────┴──────────────────────────────────────┘
```

Every message is prefixed with its length so the receiver knows exactly how many bytes to read — no delimiter scanning, no partial reads.

### Priority queue internals

```
heap entry: (-priority, counter, task)
                │           │
                │           └── tiebreaker — preserves
                │               FIFO order within same priority
                └── negated so higher priority = smaller heap value
                    (Python heapq is a min-heap)
```

### Database schema

```
tasks
├── task_id        TEXT  PRIMARY KEY
├── function_name  TEXT
├── args           TEXT  (JSON)
├── status         TEXT  (pending / running / success / failed)
├── priority       INTEGER
├── retry_count    INTEGER
└── created_time   TEXT

results
├── task_id        TEXT  PRIMARY KEY → tasks.task_id
├── result         TEXT  (JSON)
├── status         TEXT
└── finished_at    TEXT

workers
├── worker_id      TEXT  PRIMARY KEY
├── last_heartbeat REAL
└── active_task_id TEXT
```

---

## How to run

Three terminals.

```bash
# terminal 1 — broker
python -m broker.server

# terminal 2 — worker (run multiple for concurrency)
python -m worker.worker

# terminal 3 — dashboard
python -m dashboard.app
```

Submit tasks:

```bash
python main.py
```

Dashboard → http://localhost:8000

---

## Usage

Basic:

```python
from client.client import Client

client = Client()
client.submit_task("add", 2, 3, priority=5)
```

With the decorator:

```python
from client.decorators import task

@task(priority=2, retries=3)
def add(a, b):
    return a + b

r = add.delay(2, 3)
print(r.wait_for_result())   # 5
```

Async result API:

```python
r = add.delay(10, 20)

r.get_status()                  # "pending" / "running" / "success" / "failed"
r.get_result()                  # result if done, None otherwise
r.wait_for_result(timeout=10)   # blocks until done or timeout
```

---

## Project structure

```
broker/
    server.py       asyncio TCP broker, handles all connections
    queue.py        priority queue with round-robin tiebreaking
    scheduler.py    FIFO, round robin, least loaded policies

worker/
    worker.py       connects to broker, executes tasks in process pool,
                    sends heartbeats every 3s

client/
    client.py       TCP client, submits tasks to broker
    decorators.py   @task decorator, registers functions into registry
    result.py       AsyncResult — get_status, get_result, wait_for_result

shared/
    task.py         Task model
    tasks.py        task registry — maps name strings to functions
    protocol.py     encode/decode messages over TCP
    database.py     SQLite — insert, update, recover tasks
    logger.py       append-only logs for tasks and workers

dashboard/
    app.py          FastAPI server, REST endpoints, live charts

tests/
    test_integration.py   core task flow tests
    test_chaos.py         failure, flood, and retry tests

main.py             example — submit tasks and fetch results
```

---

## Tests

Two test suites, 8 tests total. All run automatically — no broker or worker needs to be started manually, the fixture spins them up as background threads.

```bash
pytest tests/
```

### Integration tests — `test_integration.py`

| Test | What it verifies |
|---|---|
| `test_submit_and_get_result` | Task submitted, executed, result correct |
| `test_priority_respected` | Higher priority task completes successfully |
| `test_unknown_task_fails` | Unknown function name returns `status=failed` |
| `test_multiple_tasks` | 5 concurrent tasks all complete with correct results |

### Chaos tests — `test_chaos.py`

| Test | What it verifies |
|---|---|
| `test_worker_crash_tasks_recovered` | Worker dies mid-task, broker detects via heartbeat timeout, task requeued and completed by new worker |
| `test_flood_queue` | 50 tasks submitted rapidly, at least 80% complete successfully without system crash or task loss |
| `test_broker_restart_recovery` | Tasks persisted to SQLite, all tracked after submission |
| `test_retry_on_failure` | Flaky tasks (70% failure rate) always reach a terminal state, system never gets stuck |

### Key testing decisions

**Polling instead of `time.sleep`**

All tests poll for results instead of sleeping for a fixed duration. Sleep-based tests are inherently flaky — they pass on fast machines and fail on slow ones. Polling exits as soon as the result is ready and fails with a clear timeout message if it isn't.

```python
# wrong — races against async execution
time.sleep(1)
assert client.get_result(task_id)["status"] == "success"

# right — waits exactly as long as needed
deadline = time.time() + 10
while time.time() < deadline:
    result = client.get_result(task_id)
    if result["status"] != "pending":
        break
    time.sleep(0.2)
assert result["status"] == "success"
```

**Why `test_retry_on_failure` submits 5 tasks**

`flaky` succeeds 30% of the time on first attempt. Testing a single task would give a 30% false-pass rate. Submitting 5 tasks reduces that to 0.3⁵ = 0.24% — essentially deterministic. The test asserts all 5 reach a terminal state, proving the system never gets stuck regardless of random failure patterns.

**Why `retry_count > 0` is not asserted**

Asserting on random behaviour makes tests non-deterministic. If all 5 flaky tasks happen to succeed on first attempt, that's correct system behaviour — not a test failure. The meaningful assertion is that tasks always resolve.

---

## Design decisions and why

**Length-prefixed protocol over raw TCP**

TCP is a stream. If you just send JSON back to back, the receiver has no idea where one message ends and the next begins. Every message is prefixed with a 4-byte header that tells the receiver exactly how many bytes to read next.

**ProcessPoolExecutor instead of threads**

Python's GIL means threads can't run CPU-bound code in parallel — only one thread executes at a time. Workers use ProcessPoolExecutor to spawn actual OS processes, each with their own GIL, so tasks run in true parallel.

**Heartbeat-based failure detection**

Workers send a heartbeat to the broker every 3 seconds. If the broker hasn't heard from a worker in 10 seconds, it marks it dead, requeues any task it was running, and removes it from the registry. No manual intervention needed.

**SQLite for persistence**

Every task is written to the database on submission. On broker restart, the first thing it does is query for tasks still in `pending` or `running` state and load them back into the queue. The system picks up exactly where it left off.

**Exponential backoff on retries**

Retrying instantly after a failure doesn't help if the underlying issue needs time to resolve. Failed tasks wait 2^retry_count seconds between attempts — 2s, 4s, 8s — before being sent to the dead letter queue after max retries.

**Priority queue with counter tiebreaker**

Python's PriorityQueue compares the full tuple on equal priorities. Comparing Task objects directly would crash since Task has no `<` operator. A monotonically increasing counter as the second tuple element breaks ties cleanly and preserves submission order within the same priority.

---

## What I learned

- How to build a TCP server from scratch using asyncio
- Why message framing matters and how to implement it
- The GIL — what it is, when it matters, and when to use processes instead of threads
- How heartbeat systems work for failure detection in distributed systems
- SQLite as a lightweight persistence layer for crash recovery
- Priority queues, scheduling algorithms, and the tradeoffs between them
- Building REST APIs with FastAPI and serving live data to a frontend
- Why sleep-based tests are flaky and how to write polling-based tests for async systems
- How distributed systems create subtle test isolation problems (stale DB state bleeding across tests)

---

## Stack

| | |
|---|---|
| Broker | Python asyncio, raw TCP sockets |
| Workers | ProcessPoolExecutor, threading |
| Persistence | SQLite |
| Dashboard | FastAPI, Chart.js |
| Protocol | JSON over length-prefixed TCP frames |
| Tests | pytest |
| Language | Python 3.13 |

---

## Install

```bash
pip install fastapi uvicorn
```

Everything else is Python standard library.

---

## Inspired by

- [Celery](https://docs.celeryq.dev/) — what this is modeled after
- [RabbitMQ](https://www.rabbitmq.com/) — broker concepts
- [Build Your Own X](https://github.com/codecrafters-io/build-your-own-x) — general philosophy
