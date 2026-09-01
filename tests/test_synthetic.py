import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

from aegispay.synthetic import (
    SCENARIOS,
    generate_customer_changes,
    generate_authentication_events,
    generate_device_intelligence_events,
    generate_access_events,
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

    def test_authentication_events_include_explainable_attack_patterns(self):
        records = generate_authentication_events(100, seed=7)
        labels = {row["scenario_label"] for row in records}
        self.assertTrue({"LEGITIMATE", "BRUTE_FORCE", "ACCOUNT_TAKEOVER"}.issubset(labels))
        self.assertTrue(any(row["mfa_result"] == "BYPASSED" for row in records))
        self.assertTrue(any(row["auth_result"] == "FAILURE" for row in records))

    def test_device_events_include_geographic_and_network_risk(self):
        records = generate_device_intelligence_events(100, seed=7)
        self.assertTrue(any(row["scenario_label"] == "IMPOSSIBLE_TRAVEL" for row in records))
        self.assertTrue(any(row["is_vpn"] for row in records))
        self.assertTrue(any(row["is_tor"] for row in records))

    def test_access_events_include_privileged_and_bulk_access(self):
        records = generate_access_events(100, seed=7)
        self.assertTrue(any(row["privileged_access"] and row["outside_business_hours"] for row in records))
        self.assertTrue(any(row["rows_accessed"] >= 25000 for row in records))

    def test_new_telemetry_generation_is_deterministic(self):
        generators = (generate_authentication_events, generate_device_intelligence_events, generate_access_events)
        for generator in generators:
            self.assertEqual(generator(50, seed=11), generator(50, seed=11))

    def test_timezone_is_required(self):
        with self.assertRaises(ValueError):
            generate_payment_events(1, start=datetime(2026, 1, 1))
        for generator in (generate_authentication_events, generate_device_intelligence_events, generate_access_events):
            with self.assertRaises(ValueError):
                generator(1, start=datetime(2026, 1, 1))

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
