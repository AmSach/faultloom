from __future__ import annotations

import io
import json
import unittest

from faultloom import FaultConfig, read_events, transform


class FaultLoomTests(unittest.TestCase):
    def test_seed_replays_identically(self) -> None:
        events = [{"id": i, "ts": float(i)} for i in range(20)]
        config = FaultConfig(drop_rate=0.2, duplicate_rate=0.2, max_delay=0.5, reorder_window=4, seed=7)
        self.assertEqual(list(transform(events, config)), list(transform(events, config)))

    def test_zero_rates_preserve_order_and_values(self) -> None:
        events = [{"id": 1, "ts": 0}, {"id": 2, "ts": 1}]
        result = list(transform(events, FaultConfig()))
        self.assertEqual([event["id"] for event in result], [1, 2])
        self.assertEqual([event["faultloom_delay"] for event in result], [0.0, 0.0])

    def test_drop_all_emits_nothing(self) -> None:
        events = [{"id": 1}, {"id": 2}]
        self.assertEqual(list(transform(events, FaultConfig(drop_rate=1.0))), [])

    def test_read_events_skips_blank_lines(self) -> None:
        stream = io.StringIO('{"id": 1}\n\n{"id": 2}\n')
        self.assertEqual(list(read_events(stream)), [{"id": 1}, {"id": 2}])

    def test_rejects_negative_timestamp(self) -> None:
        with self.assertRaises(ValueError):
            list(transform([{"ts": -1}], FaultConfig()))

    def test_cli_shape_is_json_compatible(self) -> None:
        result = list(transform([{"id": "x"}], FaultConfig(seed=1)))
        json.dumps(result)


if __name__ == "__main__":
    unittest.main()
