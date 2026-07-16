#!/usr/bin/env python3
"""Deterministic fault injection for newline-delimited JSON event streams."""

from __future__ import annotations

import argparse
import json
import random
import sys
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, TextIO


@dataclass(frozen=True)
class FaultConfig:
    """Parameters controlling deterministic stream faults.

    Args:
        drop_rate: Probability of dropping each event, from 0 to 1.
        duplicate_rate: Probability of emitting a second copy, from 0 to 1.
        max_delay: Maximum virtual delay added to each retained event.
        reorder_window: Number of events held for seeded reordering.
        seed: Seed used to make every run reproducible.
    """

    drop_rate: float = 0.0
    duplicate_rate: float = 0.0
    max_delay: float = 0.0
    reorder_window: int = 1
    seed: int = 0

    def __post_init__(self) -> None:
        """Validate configuration values before a run starts."""
        if not 0.0 <= self.drop_rate <= 1.0:
            raise ValueError("drop_rate must be between 0 and 1")
        if not 0.0 <= self.duplicate_rate <= 1.0:
            raise ValueError("duplicate_rate must be between 0 and 1")
        if self.max_delay < 0.0:
            raise ValueError("max_delay must be non-negative")
        if self.reorder_window < 1:
            raise ValueError("reorder_window must be at least 1")


def transform(events: Iterable[dict[str, Any]], config: FaultConfig) -> Iterator[dict[str, Any]]:
    """Yield a deterministic, fault-injected copy of event dictionaries.

    Args:
        events: Finite iterable of JSON-compatible dictionaries.
        config: Fault probabilities and replay seed.

    Returns:
        Iterator containing retained, duplicated, delayed, and reordered events.

    Raises:
        TypeError: If an input event is not a dictionary.
        ValueError: If an event has an invalid or negative timestamp.

    Examples:
        >>> list(transform([{"id": 1, "ts": 0.0}], FaultConfig(seed=2)))
        [{'id': 1, 'ts': 0.0}]
    """
    rng = random.Random(config.seed)
    buffer: list[dict[str, Any]] = []
    virtual_time = 0.0

    def flush(force: bool = False) -> Iterator[dict[str, Any]]:
        """Release buffered events when the reorder window is ready."""
        while buffer and (force or len(buffer) >= config.reorder_window):
            index = rng.randrange(len(buffer))
            yield buffer.pop(index)

    for original in events:
        if not isinstance(original, dict):
            raise TypeError("each event must be a JSON object")
        event = dict(original)
        timestamp = event.get("ts", virtual_time)
        if not isinstance(timestamp, (int, float)) or isinstance(timestamp, bool) or timestamp < 0:
            raise ValueError("event ts must be a non-negative number")
        virtual_time = max(virtual_time, float(timestamp))
        if rng.random() < config.drop_rate:
            continue
        virtual_time += rng.uniform(0.0, config.max_delay)
        event["faultloom_delay"] = round(virtual_time - float(timestamp), 6)
        buffer.append(event)
        yield from flush()
        if rng.random() < config.duplicate_rate:
            buffer.append(dict(event))
            yield from flush()

    yield from flush(force=True)


def read_events(stream: TextIO) -> Iterator[dict[str, Any]]:
    """Parse non-empty NDJSON lines from a text stream.

    Args:
        stream: Text stream containing one JSON object per line.

    Returns:
        Iterator of decoded JSON objects.

    Raises:
        ValueError: If a line is invalid JSON or not an object.
    """
    for line_number, line in enumerate(stream, start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"line {line_number}: invalid JSON: {exc.msg}") from exc
        if not isinstance(value, dict):
            raise ValueError(f"line {line_number}: expected a JSON object")
        yield value


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--drop-rate", type=float, default=0.0)
    parser.add_argument("--duplicate-rate", type=float, default=0.0)
    parser.add_argument("--max-delay", type=float, default=0.0)
    parser.add_argument("--reorder-window", type=int, default=1)
    parser.add_argument("--seed", type=int, default=0)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the NDJSON transformer and write NDJSON to stdout.

    Args:
        argv: Optional command-line arguments without the executable name.

    Returns:
        Process exit code: 0 for success, 2 for invalid input or configuration.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = FaultConfig(
            drop_rate=args.drop_rate,
            duplicate_rate=args.duplicate_rate,
            max_delay=args.max_delay,
            reorder_window=args.reorder_window,
            seed=args.seed,
        )
        for event in transform(read_events(sys.stdin), config):
            print(json.dumps(event, separators=(",", ":"), sort_keys=True))
        return 0
    except (TypeError, ValueError) as exc:
        print(f"faultloom: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
