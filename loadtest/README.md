# Bistro server – load test

Measures how the Bistro server behaves with **10, 25, 50 and 100 concurrent clients**
(response time, error rate, throughput), before and after making the server handle requests in parallel.

## How it works

- **Real protocol, real server.** `src/LoadTest.java` opens one **OCSF TCP connection per virtual client**
  and sends the same **Kryo-encoded `Envelope` requests** the JavaFX client sends. Every request runs the
  real server handlers and real MySQL queries (no HTTP stand-in).
- **Realistic workload.** Half of the clients log in as subscribers (PBKDF2 password check), half are guests.
  Each client repeats a customer session:
  1. today's opening hours
  2. available times for a random date (1–28 days ahead) and party size (1–4)
  3. availability check for one of those times
  4. **30% of sessions:** make the reservation, then cancel it (DB writes; keeps availability steady so
     every run does the same mix of work, however fast the server is)
  5. "my reservations" list
  6. terminal confirmation-code check
- **Closed loop, no think time.** Each client sends its next request as soon as the previous reply arrives.
- **Isolated data.** Every level starts from a freshly rebuilt `bistro_loadtest` database (`reset_db.sh`),
  never the real `bistro` database, and a freshly started headless server (MQTT and email off).
- **Phases.** 5 s ramp-up (clients connect gradually, like JMeter's ramp-up), 10 s warm-up (not counted),
  **60 s measured**. Latency = request sent → reply received.
- **Error =** timeout (30 s), dropped connection, or an error reply. Correct business answers
  ("no seats at 19:00, try 19:30") count as successes.
- **Before vs after with the same code.** "Before" runs the server with `server.workers=0`,
  `server.backlog=10`, `db.pool.maxIdle=10`, which is exactly the original behaviour (one request at a
  time inside OCSF's lock). "After" uses the defaults.

```bash
# Git Bash, from the repo root (MySQL running, Bistro_Server/server.properties present)
SERVER_WORKERS=0 SERVER_BACKLOG=10 POOL_MAX_IDLE=10 LABEL=before loadtest/run.sh
LABEL=after loadtest/run.sh
LEVELS=100 RAMPUP=0 LABEL=after-burst loadtest/run.sh     # all clients connect at the same instant
```

Results: `results/<label>.csv` (one row per operation and level, plus `ALL`).

**Test machine:** Intel Core i7-14700K (28 threads), 64 GB RAM, Windows 11 Pro, Java 25, MySQL 8.0.43.
Server, database and load generator on the same machine (localhost): network latency not included.

## Results

### Before – original server (one request at a time)

| Clients | Connected | Requests | Throughput | Avg | Median | p95 | p99 | Errors |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10  | 10/10   | 24,364 | 406 req/s | 24.6 ms  | 22.9 ms  | 46.5 ms  | 59.2 ms  | 0 % |
| 25  | 25/25   | 24,526 | 409 req/s | 61.2 ms  | 58.5 ms  | 98.0 ms  | 116.7 ms | 0 % |
| 50  | 50/50   | 24,560 | 409 req/s | 122.1 ms | 120.0 ms | 177.6 ms | 201.2 ms | 0 % |
| 100 | 100/100 | 24,769 | 413 req/s | 242.6 ms | 240.3 ms | 334.8 ms | 375.4 ms | 0 % |

### After – parallel request handling

| Clients | Connected | Requests | Throughput | Avg | Median | p95 | p99 | Errors |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10  | 10/10   | 93,635  | **1,561 req/s** | 6.4 ms  | 2.0 ms  | 32.0 ms  | 36.2 ms  | 0 % |
| 25  | 25/25   | 132,567 | **2,210 req/s** | 11.3 ms | 3.0 ms  | 59.8 ms  | 71.9 ms  | 0 % |
| 50  | 50/50   | 134,848 | **2,248 req/s** | 22.2 ms | 11.7 ms | 84.0 ms  | 110.2 ms | 0 % |
| 100 | 100/100 | 135,872 | **2,265 req/s** | 44.2 ms | 35.5 ms | 110.7 ms | 139.9 ms | 0 % |

### Improvement at 100 concurrent clients

| Metric | Before | After | Change |
|---|---:|---:|---:|
| Throughput | 413 req/s | 2,265 req/s | **5.5× higher** |
| Average response time | 242.6 ms | 44.2 ms | **5.5× lower** |
| Median response time | 240.3 ms | 35.5 ms | **6.8× lower** |
| p95 response time | 334.8 ms | 110.7 ms | **3.0× lower** |
| Clients connected in a burst (all at once) | 22 / 100 | **100 / 100** | no more refused connections |
| Error rate | 0 % | 0 % | — |

**595,141 requests measured in total (98,219 before + 496,922 after), 0 failed.**

Per operation at 100 clients (average, before → after): opening hours 241 → 30 ms,
code validation 238 → 31 ms, availability check 242 → 33 ms, cancel 242 → 33 ms,
"my reservations" 242 → 37 ms, make reservation 244 → 45 ms, available times 249 → 93 ms.

### Correctness under concurrency (no overbooking)

In a separate run (`results/archive-v1/parallel_integrity.csv`) the clients did **not** cancel, so concurrent
bookings filled the restaurant completely. After each level, `check_overbooking.sql` verified every booked
time slot (each party needs its own table big enough for it):

| Clients | Confirmed bookings | Time slots checked | Overbooked slots |
|---:|---:|---:|---:|
| 10  | 1,540 | 616 | **0** |
| 25  | 1,548 | 624 | **0** |
| 50  | 1,547 | 621 | **0** |
| 100 | 1,524 | 615 | **0** |

## Findings

1. **The original server handled one request at a time.** OCSF dispatches messages inside
   `synchronized receiveMessageFromClient`, so throughput stayed flat (~410 req/s) and latency grew in a
   straight line with the number of clients (avg ≈ clients ÷ throughput). Even the trivial "opening hours"
   request waited ~240 ms at 100 clients: time spent waiting in line, not working.
2. **Connection bursts were refused.** OCSF's listen backlog is 10 and accepting a client takes the same
   lock, so with 100 clients connecting at once only 22 got in.
3. **A bug found by the test (fixed earlier).** The deserialization security filter used lifetime-cumulative
   limits (`maxrefs`/`maxbytes`) and disconnected every client after a few hundred requests. It now uses
   per-message limits plus a class allow-list.

## What was changed

- `BistroServer.handleMessageFromClient` (called inside OCSF's lock) now only queues the request and
  returns. A worker pool (up to 32 threads) runs requests from **different clients in parallel**, while
  **each client's own requests still run in order** (one queue per connection).
- Listen backlog raised from 10 to 200 (OCSF `setBacklog`), so bursts wait instead of being refused.
- DB connection pool keeps up to 50 idle connections (was 10), so workers don't reopen MySQL connections.
- The client host-name lookup (reverse DNS, slow on a LAN) moved off the locked path.
- Booking safety is unchanged: the capacity check + insert stays inside a lock and a transaction.

## Next bottleneck

From 25 clients on, throughput levels off at ~2,250 req/s, and "available times" (~93 ms at 100 clients)
is now the slowest request: it runs the table-packing check once per half-hour slot of the day. Loading
the day's reservations once and checking all slots in memory is the next optimization to measure.

`results/archive-v1/` holds the first exploratory runs (same scenario but without the cancel step); those
are not compared with the tables above because the work mix differed.
