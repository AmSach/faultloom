# faultloom

Deterministic fault injection for newline-delimited JSON event streams. Use it to test retry logic, idempotency, ordering, and back-pressure without a live network or flaky timing.

## Features

- Drop, duplicate, delay, and reorder events.
- Seeded runs replay exactly, making failures debuggable.
- Zero runtime dependencies; Python 3.10+.
- NDJSON in and NDJSON out, so it composes with shell pipelines.
- Validates event types, timestamps, rates, and reorder windows.

## Quick start

```bash
printf '%s\n' '{"id":"a","ts":0}' '{"id":"b","ts":1}' |
  python3 faultloom.py --drop-rate 0.2 --duplicate-rate 0.1 \
    --max-delay 0.25 --reorder-window 3 --seed 42
```

The tool adds `faultloom_delay` as virtual delay metadata. The input is never mutated.

## Test

```bash
python3 -m unittest -v
```

## Why this exists

Most event-driven tests only exercise the happy path. Production failures often come from a message arriving twice, late, or out of order. faultloom turns those cases into a small, deterministic fixture you can commit to CI and reproduce locally.

## Blog draft

See `docs/blog-post.md` for the technical write-up.

## Licence

MIT.
