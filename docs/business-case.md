# Business Case

## Problem statement

Payment fraud, account takeover, and money laundering often appear across multiple events, identities, devices, merchants, and accounts. Siloed rules can miss these relationships, while overly broad controls generate too many false positives for investigators to review.

## Platform objective

AegisPay will create a unified, auditable risk decision for each eligible event by combining transaction behavior, identity relationships, policy rules, network features, and machine-learning signals.

## Primary users

- Fraud investigators need prioritized cases, evidence, and clear reason codes.
- Fraud-operations leaders need loss, alert, workload, and detection trends.
- Data engineers need reliable ingestion, reconciliation, recovery, and observability.
- Model-risk teams need reproducible evaluation, approvals, and drift monitoring.
- Governance teams need controlled access, lineage, retention, and audit evidence.

## Intended decisions

The platform recommends `APPROVE`, `REVIEW`, or `DECLINE`. It does not autonomously take irreversible action in this portfolio implementation. Investigators retain authority over cases and provide disposition feedback.

## Success criteria

| Dimension | Demonstration target |
|---|---|
| Data quality | At least 99% of valid synthetic events pass published contracts |
| Reconciliation | Bronze-to-Silver financial totals reconcile exactly for accepted events |
| Availability | Failed micro-batches can restart without duplicate decisions |
| Explainability | 100% of review and decline decisions include reason codes |
| Performance | Publish measured end-to-end latency and throughput, without invented claims |
| Model quality | Compare against a rules-only baseline and report false-positive trade-offs |
| Governance | Sensitive fields, access rules, lineage, and audit evidence are documented |

## Non-goals

- Claiming regulatory certification or production banking approval
- Using real customer or cardholder data
- Presenting synthetic evaluation results as real-world performance
- Allowing generative AI to make autonomous adverse decisions

