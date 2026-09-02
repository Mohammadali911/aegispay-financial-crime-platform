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

## Gold explainable decisioning

The serverless `AegisPay Gold Decisioning` pipeline combines observable payment,
customer, behavioral, and device-network features. A versioned policy produces a
bounded transaction risk score, risk level, recommended action, and reason-code
array without using synthetic scenario labels as decision inputs. High-risk
decisions create deterministic investigation cases, privileged or bulk access
signals create separate access alerts, and daily aggregates support operational
dashboards. Policy version and decision timestamps make every outcome reproducible
and auditable.

## MLflow model lifecycle

The `train_and_register_fraud_model` job task runs only after the Bronze, Silver,
and Gold refreshes succeed. It creates a deterministic 80/20 split keyed by event
ID, trains a class-balanced fraud-risk classifier, and logs ROC AUC, PR AUC,
precision, recall, and F1 to MLflow. The signed model is registered in Unity
Catalog as `aegispay_fraud_risk_model`, while an append-only evaluation record is
written to `ml_model_evaluation_metrics` for monitoring and auditability.

Synthetic scenario labels are used only to construct the supervised target for
this demonstration. They are excluded from the model inputs and from the existing
Gold policy decision path. Consequently, reported metrics demonstrate the model
lifecycle and must not be represented as real-world fraud performance.

The downstream `score_and_explain_fraud_model` task loads only the governed
`Champion` alias. It writes transaction probabilities, risk bands, observable
input-signal explanations, and policy-versus-model comparisons to
`ml_scored_transactions`; aggregated monitoring data is published to
`ml_scoring_metrics`. Explanations describe the elevated input evidence present
for a score and are not represented as causal claims or SHAP values. Investigator
outcomes are stored separately in the append-only, change-data-feed-enabled
`ml_analyst_feedback` table for monitoring and future retraining.

## Governed investigator access

The `secure_investigator_transactions` view is the intended analyst-facing
interface for scored payments. It excludes every direct transaction, customer,
account, merchant, device, and source-event identifier and exposes stable
environment-scoped tokens instead. This preserves investigation joins while
reducing unnecessary identity disclosure. The development implementation uses a
deterministic synthetic-data namespace; production must use a secret-backed HMAC
or enterprise tokenization service with controlled re-identification.

The `governance_control_inventory` table records evidence for least privilege,
synthetic-data classification, protected investigator access, model approval,
and analyst-feedback auditability. The DEV schema intentionally has no explicit
grants because this trial workspace has no separate governed analyst groups.

## Operational health and recovery

The final `validate_operational_health` task reconciles scored and protected row
counts, validates probability bounds and freshness, checks model-quality and
model-policy agreement thresholds, and monitors quarantine rates. It persists
every result to `operational_health_metrics` before raising a critical failure so
operators retain queryable evidence. The bundle includes a daily schedule in the
paused state for DEV cost safety. Recovery and rollback procedures are defined in
`docs/operations-runbook.md`.

## Environments

- `dev`: developer-owned resources, synthetic data, and paused schedules.
- `staging`: integration validation and production-like acceptance tests.
- `prod`: controlled schedules, production policies, and approval gates.

Production deployment will not be enabled until its permissions, cost controls, tests, and rollback procedure have been demonstrated.
