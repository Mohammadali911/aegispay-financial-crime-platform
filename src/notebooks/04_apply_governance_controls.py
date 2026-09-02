# Databricks notebook source
from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("environment", "dev")

CATALOG = dbutils.widgets.get("catalog")
ENVIRONMENT = dbutils.widgets.get("environment")
SCHEMA = f"aegispay_{ENVIRONMENT}"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.ml_scored_transactions"
SECURE_VIEW = f"{CATALOG}.{SCHEMA}.secure_investigator_transactions"
CONTROL_TABLE = f"{CATALOG}.{SCHEMA}.governance_control_inventory"


# COMMAND ----------

# This development demonstration deliberately exposes no direct party identifiers.
# A production deployment would replace the environment-scoped token namespace
# below with a secret-backed HMAC or enterprise tokenization service.
TOKEN_NAMESPACE = f"aegispay-{ENVIRONMENT}-synthetic-v1"


spark.sql(
    f"""
    CREATE OR REPLACE VIEW {SECURE_VIEW}
    COMMENT 'Investigator-safe ML decisions with pseudonymous party tokens and no direct identifiers.'
    AS SELECT
      concat('txn_', substring(sha2(concat_ws('|', '{TOKEN_NAMESPACE}', transaction_id), 256), 1, 16)) AS transaction_token,
      concat('cus_', substring(sha2(concat_ws('|', '{TOKEN_NAMESPACE}', customer_id), 256), 1, 16)) AS customer_token,
      concat('acc_', substring(sha2(concat_ws('|', '{TOKEN_NAMESPACE}', account_id), 256), 1, 16)) AS account_token,
      concat('mer_', substring(sha2(concat_ws('|', '{TOKEN_NAMESPACE}', merchant_id), 256), 1, 16)) AS merchant_token,
      concat('dev_', substring(sha2(concat_ws('|', '{TOKEN_NAMESPACE}', device_id), 256), 1, 16)) AS device_token,
      event_timestamp,
      amount,
      currency,
      payment_channel,
      fraud_probability,
      ml_prediction,
      ml_risk_level,
      ml_recommended_action,
      explanation_signals,
      policy_risk_score,
      policy_risk_level,
      policy_recommended_action,
      policy_reason_codes,
      model_policy_action_agreement,
      model_policy_risk_gap,
      registered_model_name,
      registered_model_version,
      model_alias,
      policy_version,
      scored_at
    FROM {SOURCE_TABLE}
    """
)


# COMMAND ----------

control_rows = [
    (
        "GOV-001",
        "Least privilege",
        f"{CATALOG}.{SCHEMA}",
        "No explicit schema grants in DEV; owner access only",
        "ENABLED",
    ),
    (
        "GOV-002",
        "Pseudonymized investigator access",
        SECURE_VIEW,
        "Direct transaction, customer, account, merchant, device, and event identifiers excluded",
        "ENABLED",
    ),
    (
        "GOV-003",
        "Synthetic-data classification",
        f"{CATALOG}.{SCHEMA}",
        "All demonstration data is synthetic and must not be represented as production data",
        "ENABLED",
    ),
    (
        "GOV-004",
        "Model governance",
        f"{CATALOG}.{SCHEMA}.aegispay_fraud_risk_model",
        "Scoring loads the approved Champion alias from Unity Catalog",
        "ENABLED",
    ),
    (
        "GOV-005",
        "Analyst feedback auditability",
        f"{CATALOG}.{SCHEMA}.ml_analyst_feedback",
        "Append-only feedback design with Delta Change Data Feed enabled",
        "ENABLED",
    ),
]

inventory = spark.createDataFrame(
    control_rows,
    "control_id string, control_name string, governed_object string, evidence string, control_status string",
).withColumn("validated_at", F.current_timestamp())

inventory.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    CONTROL_TABLE
)

display(inventory.orderBy("control_id"))
