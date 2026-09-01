import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

from src.aegispay.synthetic import (
    SCENARIOS,
    generate_customer_changes,
    generate_payment_events,
    to_json_lines,
)


class SyntheticDataTests(unittest.TestCase):
    def test_payment_generation_is_deterministic(self):
        self.assertEqual(generate_payment_events(25, seed=7), generate_payment_events(25, seed=7))

    def test_payment_ids_are_unique_and_scenarios_are_present(self):
        records = generate_payment_events(100)
        self.assertEqual(len(records), len({row["event_id"] for row in records}))
        self.assertTrue(set(SCENARIOS).issubset({row["scenario_label"] for row in records}))
        self.assertTrue(all(row["amount"] > 0 and row["is_synthetic"] for row in records))

    def test_customer_changes_are_ordered_and_hashed(self):
        changes = generate_customer_changes(20)
        self.assertEqual(list(range(1, 21)), [row["source_sequence"] for row in changes])
        self.assertTrue(all(len(row["email_hash"]) == 64 for row in changes))
        self.assertTrue(all("@" not in row["email_hash"] for row in changes))

    def test_timezone_is_required(self):
        with self.assertRaises(ValueError):
            generate_payment_events(1, start=datetime(2026, 1, 1))

    def test_json_lines_round_trip(self):
        records = generate_payment_events(3, start=datetime(2026, 1, 1, tzinfo=timezone.utc))
        decoded = [json.loads(line) for line in to_json_lines(records).splitlines()]
        self.assertEqual(records, decoded)

    def test_contracts_are_valid_json(self):
        root = Path(__file__).resolve().parents[1]
        for path in (root / "contracts").glob("*.json"):
            self.assertIsInstance(json.loads(path.read_text()), dict)


if __name__ == "__main__":
    unittest.main()

