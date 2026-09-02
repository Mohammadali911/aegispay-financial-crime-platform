# Databricks notebook source
from datetime import datetime, timezone

import mlflow
import mlflow.sklearn
import pandas as pd
from pyspark.sql import functions as F


dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("model_name", "aegispay_fraud_risk_model")
dbutils.widgets.text("model_alias", "Champion")

CATALOG = dbutils.widgets.get("catalog")
ENVIRONMENT = dbutils.widgets.get("environment")
SCHEMA = f"aegispay_{ENVIRONMENT}"
MODEL_NAME = dbutils.widgets.get("model_name")
MODEL_ALIAS = dbutils.widgets.get("model_alias")
REGISTERED_MODEL_NAME = f"{CATALOG}.{SCHEMA}.{MODEL_NAME}"
MODEL_URI = f"models:/{REGISTERED_MODEL_NAME}@{MODEL_ALIAS}"

NUMERIC_FEATURES = [
    "amount",
    "failed_login_count",
    "mfa_bypass_count",
    "headless_client_count",
    "anonymized_network_count",
    "untrusted_device_count",
    "impossible_travel_count",
    "linked_customer_count",
    "network_transaction_count",
]
CATEGORICAL_FEATURES = ["currency", "payment_channel", "risk_segment"]
FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES


# COMMAND ----------

payments = spark.table(f"{CATALOG}.{SCHEMA}.silver_payments").alias("p")
customers = spark.table(f"{CATALOG}.{SCHEMA}.silver_customers").select(
    "customer_id", "risk_segment"
).alias("c")
behavior = spark.table(
    f"{CATALOG}.{SCHEMA}.silver_customer_behavioral_features"
).alias("b")
network = spark.table(f"{CATALOG}.{SCHEMA}.gold_device_network_features").alias("n")

scoring_frame = (
    payments.join(customers, "customer_id", "left")
    .join(behavior, "customer_id", "left")
    .join(network, "device_id", "left")
    .fillna(0, subset=NUMERIC_FEATURES[1:])
    .fillna("UNKNOWN", subset=CATEGORICAL_FEATURES)
    .select(
        "event_id",
        "transaction_id",
        "event_timestamp",
        "customer_id",
        "account_id",
        "merchant_id",
        "device_id",
        *FEATURES,
    )
)

score_pdf = scoring_frame.toPandas()
if score_pdf.empty:
    raise ValueError("Model scoring requires at least one eligible payment")

for column in NUMERIC_FEATURES:
    score_pdf[column] = pd.to_numeric(score_pdf[column], errors="coerce").astype(float)


# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
client = mlflow.MlflowClient()
model_version = client.get_model_version_by_alias(REGISTERED_MODEL_NAME, MODEL_ALIAS)
model = mlflow.sklearn.load_model(MODEL_URI)

score_pdf["fraud_probability"] = model.predict_proba(score_pdf[FEATURES])[:, 1]
score_pdf["ml_prediction"] = (score_pdf["fraud_probability"] >= 0.5).astype(int)
score_pdf["ml_risk_level"] = pd.cut(
    score_pdf["fraud_probability"],
    bins=[-0.01, 0.35, 0.60, 0.80, 1.0],
    labels=["LOW", "MEDIUM", "HIGH", "CRITICAL"],
).astype(str)
score_pdf["ml_recommended_action"] = score_pdf["ml_risk_level"].map(
    {
        "LOW": "ALLOW",
        "MEDIUM": "STEP_UP_AUTH",
        "HIGH": "REVIEW",
        "CRITICAL": "BLOCK",
    }
)


def explanation_signals(row):
    signals = []
    if row["amount"] >= 1500:
        signals.append("HIGH_VALUE_PAYMENT")
    if row["failed_login_count"] > 0:
        signals.append("FAILED_LOGIN_ACTIVITY")
    if row["mfa_bypass_count"] > 0:
        signals.append("MFA_BYPASS")
    if row["headless_client_count"] > 0:
        signals.append("HEADLESS_CLIENT")
    if row["anonymized_network_count"] > 0:
        signals.append("ANONYMIZED_NETWORK")
    if row["untrusted_device_count"] > 0:
        signals.append("UNTRUSTED_DEVICE")
    if row["impossible_travel_count"] > 0:
        signals.append("IMPOSSIBLE_TRAVEL")
    if row["linked_customer_count"] >= 3:
        signals.append("SHARED_DEVICE_NETWORK")
    if row["risk_segment"] == "HIGH":
        signals.append("HIGH_RISK_CUSTOMER_SEGMENT")
    return signals or ["NO_ELEVATED_INPUT_SIGNAL"]


score_pdf["explanation_signals"] = score_pdf.apply(explanation_signals, axis=1)
score_pdf["registered_model_name"] = REGISTERED_MODEL_NAME
score_pdf["registered_model_version"] = str(model_version.version)
score_pdf["model_alias"] = MODEL_ALIAS
score_pdf["scored_at"] = datetime.now(timezone.utc)

scored = spark.createDataFrame(score_pdf)
policy = spark.table(f"{CATALOG}.{SCHEMA}.gold_risk_decisions").select(
    "transaction_id",
    F.col("risk_score").alias("policy_risk_score"),
    F.col("risk_level").alias("policy_risk_level"),
    F.col("recommended_action").alias("policy_recommended_action"),
    F.col("reason_codes").alias("policy_reason_codes"),
    "policy_version",
)

output = (
    scored.join(policy, "transaction_id", "left")
    .withColumn(
        "model_policy_action_agreement",
        F.col("ml_recommended_action") == F.col("policy_recommended_action"),
    )
    .withColumn(
        "model_policy_risk_gap",
        F.round(F.col("fraud_probability") * 100 - F.col("policy_risk_score"), 2),
    )
)

output.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{SCHEMA}.ml_scored_transactions"
)


# COMMAND ----------

# This append-only table is deliberately separate from model output. Investigators
# can record outcomes without overwriting the evidence used for the original score.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.ml_analyst_feedback (
      feedback_id STRING NOT NULL,
      transaction_id STRING NOT NULL,
      model_name STRING NOT NULL,
      model_version STRING NOT NULL,
      analyst_id STRING NOT NULL,
      disposition STRING NOT NULL COMMENT 'CONFIRMED_FRAUD, FALSE_POSITIVE, or NEEDS_MORE_REVIEW',
      analyst_notes STRING,
      reviewed_at TIMESTAMP NOT NULL,
      recorded_at TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Append-only investigator outcomes for model monitoring and future retraining.'
    TBLPROPERTIES (
      'aegispay.domain' = 'model-risk',
      'aegispay.data_classification' = 'synthetic',
      'delta.enableChangeDataFeed' = 'true'
    )
    """
)

summary = (
    output.groupBy(
        "registered_model_name",
        "registered_model_version",
        "model_alias",
        "ml_risk_level",
        "ml_recommended_action",
    )
    .agg(
        F.count("*").alias("scored_transaction_count"),
        F.round(F.avg("fraud_probability"), 4).alias("average_fraud_probability"),
        F.round(
            F.avg(F.col("model_policy_action_agreement").cast("double")), 4
        ).alias("policy_action_agreement_rate"),
    )
    .withColumn("metrics_calculated_at", F.current_timestamp())
)
summary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{SCHEMA}.ml_scoring_metrics"
)

display(summary.orderBy(F.desc("average_fraud_probability")))
