# Databricks notebook source
from datetime import datetime, timezone

from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("environment", "dev")

CATALOG = dbutils.widgets.get("catalog")
ENVIRONMENT = dbutils.widgets.get("environment")
SCHEMA = f"aegispay_{ENVIRONMENT}"


def table(name):
    return f"{CATALOG}.{SCHEMA}.{name}"


checks = []


def record_check(control_id, control_name, observed_value, threshold, status, severity):
    checks.append(
        (
            control_id,
            control_name,
            str(observed_value),
            threshold,
            status,
            severity,
            datetime.now(timezone.utc),
        )
    )


# COMMAND ----------

scored = spark.table(table("ml_scored_transactions"))
scored_count = scored.count()
record_check(
    "OPS-001", "Scored transaction availability", scored_count, "> 0",
    "PASS" if scored_count > 0 else "FAIL", "CRITICAL",
)

probability_bounds = scored.agg(
    F.min("fraud_probability").alias("minimum"),
    F.max("fraud_probability").alias("maximum"),
).first()
probabilities_valid = (
    probability_bounds["minimum"] is not None
    and probability_bounds["minimum"] >= 0
    and probability_bounds["maximum"] <= 1
)
record_check(
    "OPS-002", "Fraud probability bounds",
    f"{probability_bounds['minimum']}..{probability_bounds['maximum']}", "0.0..1.0",
    "PASS" if probabilities_valid else "FAIL", "CRITICAL",
)

freshness_minutes = scored.select(
    F.max(
        (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("scored_at")) / 60
    ).alias("minutes")
).first()["minutes"]
record_check(
    "OPS-003", "Scoring freshness", round(float(freshness_minutes), 2),
    "<= 120 minutes", "PASS" if freshness_minutes <= 120 else "FAIL", "CRITICAL",
)

agreement_rate = scored.agg(
    F.avg(F.col("model_policy_action_agreement").cast("double")).alias("rate")
).first()["rate"]
record_check(
    "OPS-004", "Model-policy action agreement", round(float(agreement_rate), 4),
    ">= 0.35 demonstration baseline", "PASS" if agreement_rate >= 0.35 else "WARN", "WARNING",
)


# COMMAND ----------

latest_evaluation = (
    spark.table(table("ml_model_evaluation_metrics"))
    .orderBy(F.desc("evaluated_at"))
    .select("f1", "roc_auc")
    .first()
)
evaluation_valid = latest_evaluation is not None and latest_evaluation["f1"] >= 0.60
record_check(
    "OPS-005", "Latest model evaluation",
    f"f1={latest_evaluation['f1']}, roc_auc={latest_evaluation['roc_auc']}"
    if latest_evaluation else "missing",
    "f1 >= 0.60", "PASS" if evaluation_valid else "FAIL", "CRITICAL",
)

quarantine_tables = [
    "quarantine_payment_events", "quarantine_customer_changes",
    "quarantine_authentication_events", "quarantine_device_intelligence_events",
    "quarantine_access_events",
]
bronze_tables = [
    "bronze_payment_events", "bronze_customer_changes",
    "bronze_authentication_events", "bronze_device_intelligence_events",
    "bronze_access_events",
]
quarantine_count = sum(spark.table(table(name)).count() for name in quarantine_tables)
accepted_count = sum(spark.table(table(name)).count() for name in bronze_tables)
quarantine_rate = quarantine_count / max(quarantine_count + accepted_count, 1)
record_check(
    "OPS-006", "Quarantine rate", round(quarantine_rate, 4), "<= 0.05",
    "PASS" if quarantine_rate <= 0.05 else "WARN", "WARNING",
)

protected_count = spark.table(table("secure_investigator_transactions")).count()
record_check(
    "OPS-007", "Protected investigator view availability", protected_count,
    f"= {scored_count}", "PASS" if protected_count == scored_count else "FAIL", "CRITICAL",
)


# COMMAND ----------

health = spark.createDataFrame(
    checks,
    "control_id string, control_name string, observed_value string, threshold string, control_status string, severity string, checked_at timestamp",
).withColumn("environment", F.lit(ENVIRONMENT))

health.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    table("operational_health_metrics")
)
display(health.orderBy("control_id"))

critical_failures = health.filter(
    (F.col("control_status") == "FAIL") & (F.col("severity") == "CRITICAL")
).count()
if critical_failures:
    raise RuntimeError(
        f"Operational health validation found {critical_failures} critical failure(s); "
        "review operational_health_metrics and use the documented repair procedure."
    )
