# AegisPay Interview Guide

## What did you build?

I built an end-to-end Databricks financial-crime reference platform. It ingests
five synthetic event domains, applies contracts and quarantine controls, creates
conformed identity and network features, produces explainable policy decisions,
trains and governs an ML model with MLflow and Unity Catalog, supports investigator
feedback, and publishes executive, operational, quality, and model-risk views.

## Why use Bronze, Silver, and Gold?

Bronze preserves source-aligned events and rejected records. Silver establishes
quality, deduplication, current customer state, resolved identities, and reusable
features. Gold turns those features into versioned decisions, investigation
queues, alerts, and monitoring metrics. The separation makes failures easier to
diagnose and prevents reporting logic from becoming ingestion logic.

## How did you prevent leakage?

Synthetic `scenario_label` is used only to create the supervised training target.
It is excluded from model features and completely absent from Gold policy
decisioning. The train/test split is deterministic by event ID, making evaluation
reproducible. With real data I would also use time-based validation and explicit
feature-availability checks.

## Why combine rules and ML?

Rules provide stable controls, policy ownership, and clear reasons. ML recognizes
multivariate patterns. AegisPay stores both outcomes and their agreement rather
than hiding one behind the other. Disagreement becomes a monitoring and review
signal. In the demonstration, model-policy action agreement was 42.30%.

## Why are the model metrics perfect?

The labels come from intentionally structured synthetic scenarios, so separation
is easier than in real fraud data. The scores prove that training, evaluation,
registration, signatures, metrics, and scoring are wired correctly. They do not
estimate production accuracy. Real validation would include temporal holdouts,
precision at investigator capacity, calibration, stability, subgroup analysis,
and cost-sensitive thresholds.

## What does `Champion` mean?

It is a movable Unity Catalog alias for the approved model version. Consumers load
the alias rather than hard-coding Version 1. A new candidate can be evaluated and
the alias moved only after approval; rollback moves it to the last approved
version without overwriting artifacts.

## How is model output explained?

Each scored transaction includes observable elevated-input signals such as MFA
bypass, failed-login activity, anonymized networks, untrusted devices, shared
device networks, or high-value payments. These are evidence signals, not causal
claims or SHAP values. The wording is deliberately precise.

## How did you protect sensitive data?

The analyst-facing view excludes direct transaction, customer, account, merchant,
device, and event identifiers. It exposes stable tokens so investigations can
join related activity without unnecessary identity disclosure. The repository
documents that production requires secret-backed HMAC or enterprise tokenization.

## How does the platform recover from failure?

The final task writes health-control evidence before raising critical failures.
The operator identifies the first failed task, inspects the control table, fixes
the issue through Git, reruns CI, deploys the bundle when configuration changed,
and uses Databricks Repair run. Code rollback uses Git; model rollback moves the
`Champion` alias; Delta history and Change Data Feed preserve evidence.

## What would you add before production?

- Real event sources, consent and retention controls, and a production data model
- Separate service principals and groups with least-privilege grants
- Secret-backed tokenization, governed tags, column masks, and row filters
- Time-aware model validation, calibration, drift analysis, and approval gates
- Notification destinations, on-call ownership, budgets, and SLOs
- Staging acceptance tests, load tests, disaster recovery, and formal rollback drills

## Honest deployment status

The data pipelines, dashboards, model lifecycle, governed scoring, feedback
tables, and protected view were deployed and verified in DEV. The final paused
schedule and operational-health task are committed and green in GitHub CI but
await Databricks deployment because the trial workspace exhausted its credits.
