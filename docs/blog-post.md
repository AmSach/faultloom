# I built faultloom because event systems fail in boring ways

Retry bugs are rarely caused by the happy path. A production consumer sees the same message twice, receives an older update after a newer one, or gets a burst after a connection recovers. Those cases are easy to describe and surprisingly annoying to reproduce.

`faultloom` is a tiny, dependency-free Python tool that injects those failures into newline-delimited JSON streams. It can drop, duplicate, delay, and reorder events. A seed makes the output deterministic, so a CI failure can be replayed with one command.

## The design

The input format is deliberately boring:

```json
{"id":"order-7","ts":12.0,"state":"paid"}
```

The transformer copies each event, applies a seeded random decision, and emits a new NDJSON stream. It does not sleep: delay is represented as `faultloom_delay`, which keeps tests fast and makes the simulated timeline inspectable. A bounded reorder buffer lets us model out-of-order delivery without holding the entire stream in memory.

```bash
cat events.ndjson | python3 faultloom.py \
  --drop-rate 0.05 \
  --duplicate-rate 0.10 \
  --max-delay 2.0 \
  --reorder-window 8 \
  --seed 2026
```

## Why deterministic randomness matters

A random chaos test that fails once is a frustrating anecdote. The same test with a seed is a bug report: inputs, configuration, and output can all be stored in CI artifacts. That makes it practical to test idempotency keys, sequence checks, retry budgets, and dead-letter handling.

## What it is not

faultloom is not a network emulator and does not claim to model TCP, Kafka, or a radio link. It is a focused event-stream fixture generator. If you need packet loss, bandwidth, or protocol-level timing, use a network simulator. If you need to prove that your consumer handles duplicate and reordered JSON events, this is enough.

## Try it

```bash
python3 -m unittest -v
```

The project is intentionally small: one implementation file, one test file, and no installation step. That is the point. Failure injection should be easier to add than the retry code it tests.
