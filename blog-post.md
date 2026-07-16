# Faultloom: deterministic chaos for event streams

Production event failures are often boring: a message is duplicated, delayed, dropped, or delivered out of order. Those cases are hard to reproduce when tests depend on wall-clock timing or a live broker.

Faultloom is a tiny zero-dependency Python CLI that injects those faults into newline-delimited JSON (NDJSON). It keeps the experiment deterministic: pass the same seed and the same input, and you get the same output.

## The design

The core API accepts an iterable of JSON-compatible dictionaries and a validated `FaultConfig`. Each event can be dropped or duplicated using seeded probabilities. Retained events receive virtual delay metadata rather than sleeping, so CI stays fast. A bounded buffer creates deterministic reordering without requiring a network or broker.

```bash
printf '%s\n' '{""id"":""a"",""ts"":0}\ '{""id"":""b"",""ts"":1}\ | python3 faultloom.py --drop-rate 0.2 --duplicate-rate 0.1 --max-delay 0.25 --reorder-window 3 --seed 42
```

The `faultloom_delay` field records the simulated delay. Consumers can assert retry, idempotency, ordering, and back-pressure behaviour from a fixture committed to the repository.

## Why virtual time?

Sleeping in a test makes it slow and flaky. Faultloom advances a virtual clock and annotates each event, which lets downstream code model lateness while the test process remains immediate. The trade-off is deliberate: the tool tests event semantics, not kernel scheduler behaviour.

## Safety checks

The CLI rejects malformed JSON, non-object values, negative timestamps, invalid probabilities, negative delays, and invalid reorder windows. The input dictionaries are copied before metadata is added, so callers do not see surprising mutations.

## Try it

The project is MIT-licensed and runs on Python 3.10+ with no third-party packages. Run `python3 -m unittest -v` to execute the six regression tests.

Repository: https://github.com/AmSach/faultloom
