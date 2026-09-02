# Databricks notebook source
from datetime import datetime, timezone

import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow.models import infer_signature
from pyspark.sql import functions as F
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("model_name", "aegispay_fraud_risk_model")
dbutils.widgets.text("model_version", "2026.09.1")

CATALOG = dbutils.widgets.get("catalog")
ENVIRONMENT = dbutils.widgets.get("environment")
SCHEMA = f"aegispay_{ENVIRONMENT}"
MODEL_NAME = dbutils.widgets.get("model_name")
MODEL_VERSION = dbutils.widgets.get("model_version")
REGISTERED_MODEL_NAME = f"{CATALOG}.{SCHEMA}.{MODEL_NAME}"
EXPERIMENT_NAME = f"/Shared/aegispay-{ENVIRONMENT}-fraud-risk-training"

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
behavior = spark.table(f"{CATALOG}.{SCHEMA}.silver_customer_behavioral_features").alias("b")
network = spark.table(f"{CATALOG}.{SCHEMA}.gold_device_network_features").alias("n")

training_frame = (
    payments.join(customers, "customer_id", "left")
    .join(behavior, "customer_id", "left")
    .join(network, "device_id", "left")
    .withColumn("label", (F.col("scenario_label") != "LEGITIMATE").cast("int"))
    .withColumn("split_bucket", F.pmod(F.xxhash64("event_id"), F.lit(10)))
    .fillna(
        0,
        subset=[
            "failed_login_count",
            "mfa_bypass_count",
            "headless_client_count",
            "anonymized_network_count",
            "untrusted_device_count",
            "impossible_travel_count",
            "linked_customer_count",
            "network_transaction_count",
        ],
    )
    .fillna("UNKNOWN", subset=["currency", "payment_channel", "risk_segment"])
    .select("event_id", "event_timestamp", "label", "split_bucket", *FEATURES)
)

class_counts = {row["label"]: row["count"] for row in training_frame.groupBy("label").count().collect()}
if set(class_counts) != {0, 1}:
    raise ValueError(f"Training requires both classes; observed counts: {class_counts}")

train_pdf = training_frame.filter(F.col("split_bucket") < 8).drop("split_bucket").toPandas()
test_pdf = training_frame.filter(F.col("split_bucket") >= 8).drop("split_bucket").toPandas()
if train_pdf.empty or test_pdf.empty or test_pdf["label"].nunique() != 2:
    raise ValueError("Deterministic train/test split must contain test rows from both classes")


# COMMAND ----------

numeric_pipeline = Pipeline(
    steps=[("imputer", SimpleImputer(strategy="median"))]
)
categorical_pipeline = Pipeline(
    steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("one_hot", OneHotEncoder(handle_unknown="ignore")),
    ]
)
preprocessor = ColumnTransformer(
    transformers=[
        ("numeric", numeric_pipeline, NUMERIC_FEATURES),
        ("categorical", categorical_pipeline, CATEGORICAL_FEATURES),
    ]
)
model = Pipeline(
    steps=[
        ("preprocessor", preprocessor),
        (
            "classifier",
            RandomForestClassifier(
                n_estimators=200,
                max_depth=8,
                min_samples_leaf=3,
                class_weight="balanced",
                random_state=42,
                n_jobs=-1,
            ),
        ),
    ]
)

X_train = train_pdf[FEATURES]
y_train = train_pdf["label"]
X_test = test_pdf[FEATURES]
y_test = test_pdf["label"]

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT_NAME)

with mlflow.start_run(run_name=f"fraud-risk-{MODEL_VERSION}") as run:
    model.fit(X_train, y_train)
    probabilities = model.predict_proba(X_test)[:, 1]
    predictions = (probabilities >= 0.5).astype(int)

    metrics = {
        "roc_auc": float(roc_auc_score(y_test, probabilities)),
        "pr_auc": float(average_precision_score(y_test, probabilities)),
        "precision": float(precision_score(y_test, predictions, zero_division=0)),
        "recall": float(recall_score(y_test, predictions, zero_division=0)),
        "f1": float(f1_score(y_test, predictions, zero_division=0)),
        "train_row_count": float(len(train_pdf)),
        "test_row_count": float(len(test_pdf)),
        "positive_class_rate": float(y_train.mean()),
    }
    mlflow.log_metrics(metrics)
    mlflow.log_params(
        {
            "model_version": MODEL_VERSION,
            "label_definition": "scenario_label != LEGITIMATE",
            "split_strategy": "xxhash64(event_id): 80/20",
            "decision_threshold": 0.5,
            "training_data": f"{CATALOG}.{SCHEMA}.silver_payments",
            "synthetic_training_data": True,
        }
    )
    mlflow.set_tags(
        {
            "project": "aegispay",
            "environment": ENVIRONMENT,
            "data_classification": "synthetic",
            "intended_use": "portfolio demonstration; not production financial advice",
        }
    )

    input_example = X_train.head(5)
    signature = infer_signature(input_example, model.predict_proba(input_example)[:, 1])
    model_info = mlflow.sklearn.log_model(
        sk_model=model,
        artifact_path="fraud_risk_model",
        registered_model_name=REGISTERED_MODEL_NAME,
        signature=signature,
        input_example=input_example,
    )
    run_id = run.info.run_id


# COMMAND ----------

evaluated_at = datetime.now(timezone.utc)
evaluation_row = {
    "model_name": REGISTERED_MODEL_NAME,
    "model_version_label": MODEL_VERSION,
    "mlflow_run_id": run_id,
    "model_uri": model_info.model_uri,
    "evaluated_at": evaluated_at,
    "roc_auc": metrics["roc_auc"],
    "pr_auc": metrics["pr_auc"],
    "precision": metrics["precision"],
    "recall": metrics["recall"],
    "f1": metrics["f1"],
    "train_row_count": int(metrics["train_row_count"]),
    "test_row_count": int(metrics["test_row_count"]),
    "positive_class_rate": metrics["positive_class_rate"],
    "data_classification": "SYNTHETIC",
}
spark.createDataFrame(pd.DataFrame([evaluation_row])).write.mode("append").saveAsTable(
    f"{CATALOG}.{SCHEMA}.ml_model_evaluation_metrics"
)

display(spark.createDataFrame(pd.DataFrame([evaluation_row])))
