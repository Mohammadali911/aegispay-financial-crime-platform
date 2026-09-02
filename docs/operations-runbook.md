# AegisPay Operations and Recovery Runbook

## Operating posture

The DEV schedule is deployed in a paused state. This preserves a production-style
orchestration definition without consuming trial compute automatically. Enabling
STAGING or PROD schedules requires an approved owner, budget, notification
destination, access review, and rollback test.

## Health controls

The final job task writes `operational_health_metrics` and validates:

1. scored transactions exist;
2. fraud probabilities remain between zero and one;
3. scoring completed within 120 minutes;
4. model-policy agreement remains above the demonstration baseline;
5. the latest registered model meets the minimum F1 threshold;
6. quarantine volume remains within tolerance; and
7. the protected investigator view reconciles to the scored population.

Warning controls are visible but do not stop processing. Critical failures are
written to the health table before the task fails.

## Recovery procedure

1. Open the failed AegisPay job run and identify the first failed task.
2. Read the task error and `operational_health_metrics`; do not repeatedly retry
   without identifying the failed control.
3. Confirm the Git commit and Databricks bundle deployment correspond to the
   intended release.
4. Correct the source, configuration, permissions, or upstream data issue through
   version-controlled code.
5. Run local tests, commit and push the correction, pull it into Databricks, and
   deploy the bundle when a resource definition changed.
6. Use **Repair run** to rerun the failed task and its downstream dependencies.
7. Reconcile row counts and health controls, then capture the successful run ID as
   recovery evidence.

## Rollback procedure

Application and pipeline code is rolled back by reverting the faulty Git commit,
redeploying the bundle, and repairing or starting a controlled validation run.
Models are rolled back by moving the Unity Catalog `Champion` alias to the last
approved version; model artifacts are never overwritten. Delta history and Change
Data Feed preserve data and feedback evidence. Destructive table replacement is
not an approved recovery technique.

## Interview summary

> I designed failure detection into the final orchestration task. It persists
> control evidence before failing, separates warnings from critical conditions,
> keeps automated DEV execution paused for cost control, and uses Git-based fixes,
> bundle deployment, Databricks repair runs, Delta history, and model aliases for
> controlled recovery and rollback.
