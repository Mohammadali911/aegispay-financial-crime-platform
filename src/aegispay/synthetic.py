"""Deterministic synthetic data for AegisPay demonstrations.

The generator intentionally creates labeled patterns for engineering and detection
tests. It does not model real people and must not be used to claim production
model performance.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from ipaddress import IPv4Address
from random import Random
from typing import Iterable
from uuid import NAMESPACE_URL, uuid5


SCENARIOS = (
    "LEGITIMATE",
    "PAYMENT_FRAUD",
    "ACCOUNT_TAKEOVER",
    "MULE_NETWORK",
    "LAYERING",
)


def _identifier(kind: str, seed: int, index: int) -> str:
    return f"{kind}_{uuid5(NAMESPACE_URL, f'aegispay:{seed}:{kind}:{index}').hex[:16]}"


def _hash(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class PaymentEvent:
    event_id: str
    event_timestamp: str
    transaction_id: str
    customer_id: str
    account_id: str
    merchant_id: str
    device_id: str
    ip_address: str
    amount: float
    currency: str
    payment_channel: str
    event_type: str
    source_sequence: int
    is_synthetic: bool
    scenario_label: str


@dataclass(frozen=True)
class CustomerChange:
    change_id: str
    change_timestamp: str
    operation: str
    customer_id: str
    risk_segment: str | None
    country: str | None
    email_hash: str | None
    phone_hash: str | None
    source_sequence: int
    is_synthetic: bool


def generate_payment_events(
    count: int,
    *,
    seed: int = 20260831,
    start: datetime | None = None,
) -> list[dict]:
    """Generate repeatable payments with labeled, testable crime scenarios."""
    if count < 1:
        raise ValueError("count must be positive")

    rng = Random(seed)
    start = start or datetime(2026, 1, 1, tzinfo=timezone.utc)
    if start.tzinfo is None:
        raise ValueError("start must be timezone-aware")

    events: list[dict] = []
    for index in range(count):
        sequence = index + 1
        scenario = SCENARIOS[index % 20] if index % 20 < 5 else "LEGITIMATE"
        customer_slot = index % 40
        device_slot = customer_slot
        merchant_slot = index % 12
        amount = round(rng.uniform(8, 480), 2)
        channel = rng.choice(["CARD_PRESENT", "ECOMMERCE", "MOBILE", "TRANSFER"])
        event_type = "TRANSFER" if channel == "TRANSFER" else "AUTHORISATION"

        if scenario == "PAYMENT_FRAUD":
            amount, channel, device_slot = round(rng.uniform(900, 2400), 2), "ECOMMERCE", 900
        elif scenario == "ACCOUNT_TAKEOVER":
            amount, channel, device_slot = round(rng.uniform(700, 1800), 2), "MOBILE", 901
        elif scenario == "MULE_NETWORK":
            amount, channel, customer_slot, device_slot = round(rng.uniform(450, 900), 2), "TRANSFER", index % 5, 950
        elif scenario == "LAYERING":
            amount, channel, customer_slot = round(rng.uniform(200, 700), 2), "TRANSFER", index % 6

        timestamp = start + timedelta(seconds=index * 15)
        event = PaymentEvent(
            event_id=_identifier("evt", seed, sequence),
            event_timestamp=timestamp.isoformat().replace("+00:00", "Z"),
            transaction_id=_identifier("txn", seed, sequence),
            customer_id=_identifier("cus", seed, customer_slot),
            account_id=_identifier("acc", seed, customer_slot),
            merchant_id=_identifier("mer", seed, merchant_slot),
            device_id=_identifier("dev", seed, device_slot),
            ip_address=str(IPv4Address(0x0A000001 + (device_slot % 250))),
            amount=amount,
            currency=rng.choice(["CAD", "USD", "EUR", "GBP"]),
            payment_channel=channel,
            event_type=event_type,
            source_sequence=sequence,
            is_synthetic=True,
            scenario_label=scenario,
        )
        events.append(asdict(event))
    return events


def generate_customer_changes(
    count: int,
    *,
    seed: int = 20260831,
    start: datetime | None = None,
) -> list[dict]:
    """Generate ordered customer CDC events with privacy-safe identifiers."""
    if count < 1:
        raise ValueError("count must be positive")
    start = start or datetime(2026, 1, 1, tzinfo=timezone.utc)
    if start.tzinfo is None:
        raise ValueError("start must be timezone-aware")

    changes: list[dict] = []
    for index in range(count):
        sequence = index + 1
        customer_slot = index % max(1, count // 2)
        operation = "INSERT" if index < max(1, count // 2) else "UPDATE"
        customer_id = _identifier("cus", seed, customer_slot)
        change = CustomerChange(
            change_id=_identifier("chg", seed, sequence),
            change_timestamp=(start + timedelta(minutes=index)).isoformat().replace("+00:00", "Z"),
            operation=operation,
            customer_id=customer_id,
            risk_segment="HIGH" if index % 11 == 0 else "LOW",
            country=("CA", "US", "GB", "DE")[index % 4],
            email_hash=_hash(f"synthetic-{customer_slot}@example.invalid"),
            phone_hash=_hash(f"+1555000{customer_slot:04d}"),
            source_sequence=sequence,
            is_synthetic=True,
        )
        changes.append(asdict(change))
    return changes


def to_json_lines(records: Iterable[dict]) -> str:
    """Serialize records as stable newline-delimited JSON."""
    import json

    return "\n".join(json.dumps(record, sort_keys=True) for record in records) + "\n"

