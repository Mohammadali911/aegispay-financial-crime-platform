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

## Environments

- `dev`: developer-owned resources, synthetic data, and paused schedules.
- `staging`: integration validation and production-like acceptance tests.
- `prod`: controlled schedules, production policies, and approval gates.

Production deployment will not be enabled until its permissions, cost controls, tests, and rollback procedure have been demonstrated.

