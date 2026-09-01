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


@dataclass(frozen=True)
class AuthenticationEvent:
    auth_event_id: str
    event_timestamp: str
    customer_id: str
    account_id: str
    session_id: str
    device_id: str
    ip_address: str
    country: str
    city: str
    auth_result: str
    failure_reason: str | None
    mfa_result: str
    user_agent: str
    source_sequence: int
    is_synthetic: bool
    scenario_label: str


@dataclass(frozen=True)
class DeviceIntelligenceEvent:
    device_event_id: str
    observed_at: str
    customer_id: str
    device_id: str
    ip_address: str
    country: str
    city: str
    latitude: float
    longitude: float
    network_type: str
    is_vpn: bool
    is_tor: bool
    device_trust: str
    source_sequence: int
    is_synthetic: bool
    scenario_label: str


@dataclass(frozen=True)
class AccessEvent:
    access_event_id: str
    event_timestamp: str
    actor_id: str
    actor_type: str
    role_name: str
    source_ip: str
    resource_type: str
    resource_name: str
    action: str
    access_result: str
    rows_accessed: int
    privileged_access: bool
    outside_business_hours: bool
    source_sequence: int
    is_synthetic: bool
    scenario_label: str


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


def generate_authentication_events(
    count: int,
    *,
    seed: int = 20260831,
    start: datetime | None = None,
) -> list[dict]:
    """Generate login, MFA, brute-force, and account-takeover telemetry."""
    if count < 1:
        raise ValueError("count must be positive")
    start = start or datetime(2026, 1, 1, tzinfo=timezone.utc)
    if start.tzinfo is None:
        raise ValueError("start must be timezone-aware")

    events: list[dict] = []
    locations = (("CA", "Toronto"), ("US", "New York"), ("GB", "London"), ("DE", "Berlin"))
    for index in range(count):
        sequence = index + 1
        customer_slot = index % 40
        scenario = "LEGITIMATE"
        auth_result, failure_reason, mfa_result = "SUCCESS", None, "PASSED"
        device_slot = customer_slot
        country, city = locations[customer_slot % len(locations)]
        if index % 25 in range(5):
            scenario = "BRUTE_FORCE"
            auth_result, failure_reason, mfa_result = "FAILURE", "INVALID_PASSWORD", "NOT_CHALLENGED"
            device_slot = 980
        elif index % 25 == 5:
            scenario = "ACCOUNT_TAKEOVER"
            device_slot, country, city, mfa_result = 981, "NL", "Amsterdam", "BYPASSED"
        timestamp = start + timedelta(seconds=index * 20)
        event = AuthenticationEvent(
            auth_event_id=_identifier("auth", seed, sequence),
            event_timestamp=timestamp.isoformat().replace("+00:00", "Z"),
            customer_id=_identifier("cus", seed, customer_slot),
            account_id=_identifier("acc", seed, customer_slot),
            session_id=_identifier("ses", seed, sequence),
            device_id=_identifier("dev", seed, device_slot),
            ip_address=str(IPv4Address(0x0A010001 + (device_slot % 250))),
            country=country,
            city=city,
            auth_result=auth_result,
            failure_reason=failure_reason,
            mfa_result=mfa_result,
            user_agent="SyntheticBrowser/1.0" if scenario == "LEGITIMATE" else "HeadlessSynthetic/1.0",
            source_sequence=sequence,
            is_synthetic=True,
            scenario_label=scenario,
        )
        events.append(asdict(event))
    return events


def generate_device_intelligence_events(
    count: int,
    *,
    seed: int = 20260831,
    start: datetime | None = None,
) -> list[dict]:
    """Generate device, network, VPN/Tor, and geographic-risk observations."""
    if count < 1:
        raise ValueError("count must be positive")
    start = start or datetime(2026, 1, 1, tzinfo=timezone.utc)
    if start.tzinfo is None:
        raise ValueError("start must be timezone-aware")

    events: list[dict] = []
    locations = (
        ("CA", "Toronto", 43.6532, -79.3832),
        ("US", "New York", 40.7128, -74.0060),
        ("GB", "London", 51.5072, -0.1276),
        ("DE", "Berlin", 52.5200, 13.4050),
    )
    for index in range(count):
        sequence = index + 1
        customer_slot = index % 40
        country, city, latitude, longitude = locations[customer_slot % len(locations)]
        scenario, is_vpn, is_tor, trust, network_type = "LEGITIMATE", False, False, "TRUSTED", "RESIDENTIAL"
        device_slot = customer_slot
        if index % 30 == 0:
            scenario, is_vpn, trust, network_type = "IMPOSSIBLE_TRAVEL", True, "UNKNOWN", "VPN"
            country, city, latitude, longitude, device_slot = "SG", "Singapore", 1.3521, 103.8198, 990
        elif index % 30 == 1:
            scenario, is_tor, trust, network_type, device_slot = "ANONYMIZED_NETWORK", True, "BLOCKED", "TOR", 991
        event = DeviceIntelligenceEvent(
            device_event_id=_identifier("dvi", seed, sequence),
            observed_at=(start + timedelta(seconds=index * 30)).isoformat().replace("+00:00", "Z"),
            customer_id=_identifier("cus", seed, customer_slot),
            device_id=_identifier("dev", seed, device_slot),
            ip_address=str(IPv4Address(0x0A020001 + (device_slot % 250))),
            country=country,
            city=city,
            latitude=latitude,
            longitude=longitude,
            network_type=network_type,
            is_vpn=is_vpn,
            is_tor=is_tor,
            device_trust=trust,
            source_sequence=sequence,
            is_synthetic=True,
            scenario_label=scenario,
        )
        events.append(asdict(event))
    return events


def generate_access_events(
    count: int,
    *,
    seed: int = 20260831,
    start: datetime | None = None,
) -> list[dict]:
    """Generate employee, privileged-access, and database/API audit events."""
    if count < 1:
        raise ValueError("count must be positive")
    start = start or datetime(2026, 1, 1, tzinfo=timezone.utc)
    if start.tzinfo is None:
        raise ValueError("start must be timezone-aware")

    events: list[dict] = []
    for index in range(count):
        sequence = index + 1
        actor_slot = index % 20
        scenario, rows, privileged, outside = "LEGITIMATE", (index % 25) + 1, False, False
        action, role, resource_type = "READ", "ANALYST", "CASE"
        if index % 40 == 0:
            scenario, rows, privileged, outside = "PRIVILEGED_ABUSE", 50000, True, True
            action, role, resource_type = "EXPORT", "PLATFORM_ADMIN", "CUSTOMER_TABLE"
        elif index % 40 == 1:
            scenario, rows, outside = "DATABASE_ANOMALY", 25000, True
            action, resource_type = "BULK_READ", "PAYMENT_TABLE"
        event = AccessEvent(
            access_event_id=_identifier("acs", seed, sequence),
            event_timestamp=(start + timedelta(seconds=index * 45)).isoformat().replace("+00:00", "Z"),
            actor_id=_identifier("usr", seed, actor_slot),
            actor_type="EMPLOYEE",
            role_name=role,
            source_ip=str(IPv4Address(0x0A030001 + actor_slot)),
            resource_type=resource_type,
            resource_name="synthetic_" + resource_type.lower(),
            action=action,
            access_result="ALLOWED",
            rows_accessed=rows,
            privileged_access=privileged,
            outside_business_hours=outside,
            source_sequence=sequence,
            is_synthetic=True,
            scenario_label=scenario,
        )
        events.append(asdict(event))
    return events


def to_json_lines(records: Iterable[dict]) -> str:
    """Serialize records as stable newline-delimited JSON."""
    import json

    return "\n".join(json.dumps(record, sort_keys=True) for record in records) + "\n"
