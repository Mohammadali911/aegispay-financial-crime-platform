# AegisPay Demonstration Walkthrough

This walkthrough presents the project in approximately five minutes. It uses
synthetic data and avoids claims of production model accuracy.

## 1. Frame the problem

Financial-crime systems must do more than calculate a fraud score. They must
reliably ingest events, reconcile changing identities, connect suspicious
entities, explain actions, protect identifiers, support investigators, and remain
recoverable and auditable.

## 2. Show the data platform

Open the Databricks job graph and explain the ordered flow:

1. validate the target environment;
2. generate deterministic synthetic payments, customer changes, authentication,
   device-intelligence, and access events;
3. refresh Bronze ingestion and quarantine;
4. refresh Silver conformance, identity, behavior, and graph features;
5. refresh Gold policy decisions and investigation products;
6. train and register the model;
7. score with the governed `Champion` alias;
8. apply protected investigator access; and
9. validate operational health.

The schedule is defined but paused in DEV to demonstrate cost control.

## 3. Show explainable decisioning

Open `gold_risk_decisions`. Highlight the bounded risk score, risk level,
recommended action, reason-code array, policy version, and decision timestamp.
Explain that synthetic scenario labels never enter this policy path.

Then show `gold_investigation_queue` and `gold_access_alerts` to demonstrate that
transaction fraud and privileged-access risk become separate operational work.

## 4. Show the ML lifecycle

Open the Unity Catalog model `aegispay_fraud_risk_model` and Version 1. Show the
Ready status, signature, parameters, MLflow source run, and `Champion` alias.
Explain the deterministic 80/20 split and why perfect scores on structured
synthetic scenarios are workflow-validation evidence rather than production
performance evidence.

Open `ml_scored_transactions` and highlight fraud probability, ML risk/action,
observable explanation signals, the policy action, and the agreement indicator.
The 42.30% agreement rate demonstrates that the ML and policy paths are genuinely
different controls rather than duplicated labels.

## 5. Show governance and operations

Open `secure_investigator_transactions`. Demonstrate that transaction, customer,
account, merchant, and device identifiers are replaced with stable pseudonymous
tokens. Explain that production would use secret-backed HMAC or enterprise
tokenization rather than the development namespace.

Show the command-center pages:

- Executive Overview
- Investigation Operations
- Data Quality & Platform Health
- Model Risk & Performance

Finish with GitHub Actions and the recovery runbook. A failed critical health
control is persisted before the job fails; operators diagnose it, fix the source
through Git, deploy the bundle, and use a Databricks repair run. Model rollback is
performed by moving the `Champion` alias to the last approved version.

## Closing statement

> AegisPay demonstrates an end-to-end financial-crime operating model: governed
> data engineering, explainable rules and ML, investigation-ready outputs,
> controlled access, observable health, and version-controlled recovery.
