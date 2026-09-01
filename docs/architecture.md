# Architecture Decisions

## Principles

1. Preserve source evidence before transformation.
2. Make every write idempotent and every decision reproducible.
3. Separate detection signals from the final decision policy.
4. Treat explainability and auditability as required outputs.
5. Use synthetic data and least-privilege access by default.
6. Promote the same versioned artifact through all environments.

## Logical layers

- **Landing:** immutable source files and streaming checkpoints.
- **Bronze:** append-oriented source records with ingestion metadata.
- **Silver:** validated, deduplicated, conformed entities and events.
- **Feature:** point-in-time behavioral and graph-derived signals.
- **Decision:** rule hits, model scores, policy outcomes, and reason codes.
- **Gold:** cases, operational KPIs, model monitoring, and executive metrics.
- **Quarantine:** rejected records with failure reasons and remediation state.

## Bronze implementation

The serverless `AegisPay Bronze Ingestion` Lakeflow pipeline reads the synthetic
payment and customer-change Delta sources incrementally. Explicit projections
enforce the contracts, expectations drop invalid rows from trusted Bronze
tables, parallel quarantine tables retain rejected records and reason codes,
and AUTO CDC maintains the latest customer state using ordered SCD Type 1
semantics. Pipeline refreshes run only after the source-generation task succeeds.

## Silver implementation

The separate serverless `AegisPay Silver Conformance` pipeline isolates trusted
business entities from Bronze evidence. It uses event-time watermarks and the
contracted event ID to bound streaming deduplication, resolves customer identities
with deterministic hashes of protected identifiers, and emits aggregated
customer-account, account-merchant, and customer-device edges for downstream
graph features. The workflow refreshes Silver only after Bronze succeeds.

## Behavioral and access-risk telemetry

Milestone 5 adds versioned authentication, device/IP intelligence, and access-audit
contracts. Deterministic synthetic sources exercise repeated login failures, MFA
bypass, headless clients, impossible travel, VPN/Tor use, untrusted devices,
privileged after-hours activity, and anomalous bulk data access. Bronze applies
domain-specific expectations and quarantine reason codes. Silver deduplicates each
stream and publishes customer behavioral features plus employee/service access-risk
signals with bounded scores and human-readable reason codes. These features are
inputs to decisioning; synthetic scenario labels remain validation-only fields.

## Environments

- `dev`: developer-owned resources, synthetic data, and paused schedules.
- `staging`: integration validation and production-like acceptance tests.
- `prod`: controlled schedules, production policies, and approval gates.

Production deployment will not be enabled until its permissions, cost controls, tests, and rollback procedure have been demonstrated.
