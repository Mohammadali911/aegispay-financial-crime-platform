# Threat Model

## Protected assets

- Customer identity and authentication attributes
- Account and payment information
- Watchlist matches and investigation evidence
- Risk models, rules, thresholds, and decisions
- Analyst dispositions and audit records

## Principal abuse scenarios

1. Stolen credentials are used from a new device to take over an account.
2. A fraud ring reuses devices, IP addresses, addresses, or merchants across identities.
3. Mule accounts rapidly receive and disperse funds.
4. Transfers are layered through circular or fan-in/fan-out networks.
5. Invalid or duplicated events manipulate balances or detection features.
6. Unauthorized users access sensitive fields or investigation records.
7. Model or rule changes are deployed without validation or traceability.
8. Prompt injection or untrusted evidence misleads an investigation assistant.

## Design controls

- Least-privilege Unity Catalog access and environment isolation
- Masking and row filtering for sensitive data
- Immutable raw events, idempotent processing, and reconciliation
- Versioned rules, features, models, and decision evidence
- Quarantine for malformed or policy-invalid events
- Human approval for consequential investigation outcomes
- CI quality gates and controlled bundle deployments
- Grounded AI summaries that distinguish evidence from recommendations
- Audit records for access, deployment, scoring, and analyst actions

## Trust boundaries

External source events are untrusted until validated. Bronze records preserve received evidence; Silver tables contain contract-valid data; Gold outputs are authorized business products. Development data and permissions remain isolated from staging and production targets.

