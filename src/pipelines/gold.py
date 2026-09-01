from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_CATALOG = spark.conf.get("aegispay.source_catalog")
SOURCE_SCHEMA = spark.conf.get("aegispay.source_schema")
POLICY_VERSION = spark.conf.get("aegispay.policy_version", "2026.09.1")

SILVER_PAYMENTS = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.silver_payments"
SILVER_CUSTOMERS = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.silver_customers"
SILVER_EDGES = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.silver_transaction_edges"
SILVER_BEHAVIOR = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.silver_customer_behavioral_features"
SILVER_ACCESS = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.silver_access_risk_signals"


@dp.materialized_view(
    name="gold_device_network_features",
    comment="Device-sharing and transaction-network features used by explainable decisioning.",
    table_properties={"quality": "gold", "aegispay.feature_group": "network-risk"},
)
def gold_device_network_features():
    return (
        spark.read.table(SILVER_EDGES)
        .filter((F.col("edge_type") == "USES") & (F.col("target_type") == "DEVICE"))
        .groupBy(F.col("target_id").alias("device_id"))
        .agg(
            F.countDistinct("source_id").alias("linked_customer_count"),
            F.sum("transaction_count").alias("network_transaction_count"),
            F.round(F.sum("total_amount"), 2).alias("network_total_amount"),
            F.max("last_seen_at").alias("network_last_seen_at"),
        )
        .withColumn("is_shared_device", F.col("linked_customer_count") >= 3)
        .withColumn("network_feature_calculated_at", F.current_timestamp())
    )


@dp.materialized_view(
    name="gold_risk_decisions",
    comment="Transaction-level explainable financial-crime decisions with policy outcomes and evidence.",
    table_properties={
        "quality": "gold",
        "aegispay.domain": "financial-crime-decisioning",
        "aegispay.policy_version": POLICY_VERSION,
    },
)
@dp.expect_or_fail("decision_identity_complete", "decision_id IS NOT NULL AND transaction_id IS NOT NULL")
@dp.expect_or_fail("risk_score_bounded", "risk_score BETWEEN 0 AND 100")
@dp.expect_or_fail("action_supported", "recommended_action IN ('ALLOW', 'STEP_UP_AUTH', 'REVIEW', 'BLOCK')")
def gold_risk_decisions():
    payments = spark.read.table(SILVER_PAYMENTS).alias("p")
    customers = spark.read.table(SILVER_CUSTOMERS).select("customer_id", "risk_segment", "resolved_identity_id").alias("c")
    behavior = spark.read.table(SILVER_BEHAVIOR).alias("b")
    devices = spark.read.table("gold_device_network_features").alias("d")

    enriched = (
        payments.join(customers, "customer_id", "left")
        .join(behavior, "customer_id", "left")
        .join(devices, "device_id", "left")
        .fillna({
            "behavioral_risk_score": 0,
            "linked_customer_count": 0,
            "network_transaction_count": 0,
            "network_total_amount": 0.0,
            "is_shared_device": False,
            "risk_segment": "UNKNOWN",
        })
        .withColumn("amount_rule_score", F.when(F.col("amount") >= 1500, 25).when(F.col("amount") >= 750, 10).otherwise(0))
        .withColumn("channel_rule_score", F.when(F.col("payment_channel").isin("ECOMMERCE", "MOBILE"), 5).otherwise(0))
        .withColumn("shared_device_rule_score", F.when(F.col("is_shared_device"), 25).otherwise(0))
        .withColumn("customer_segment_score", F.when(F.col("risk_segment") == "HIGH", 10).otherwise(0))
        .withColumn(
            "risk_score",
            F.least(
                F.lit(100),
                F.col("behavioral_risk_score")
                + F.col("amount_rule_score")
                + F.col("channel_rule_score")
                + F.col("shared_device_rule_score")
                + F.col("customer_segment_score"),
            ).cast("int"),
        )
        .withColumn(
            "transaction_reason_codes",
            F.array_compact(F.array(
                F.when(F.col("amount") >= 1500, F.lit("HIGH_VALUE_PAYMENT")),
                F.when((F.col("amount") >= 750) & (F.col("amount") < 1500), F.lit("ELEVATED_VALUE_PAYMENT")),
                F.when(F.col("payment_channel").isin("ECOMMERCE", "MOBILE"), F.lit("REMOTE_PAYMENT_CHANNEL")),
                F.when(F.col("is_shared_device"), F.lit("SHARED_DEVICE_NETWORK")),
                F.when(F.col("risk_segment") == "HIGH", F.lit("HIGH_RISK_CUSTOMER_SEGMENT")),
            )),
        )
        .withColumn("behavior_reason_codes", F.coalesce(F.col("reason_codes"), F.array().cast("array<string>")))
        .withColumn("reason_codes", F.array_distinct(F.concat(F.col("transaction_reason_codes"), F.col("behavior_reason_codes"))))
        .withColumn(
            "risk_level",
            F.when(F.col("risk_score") >= 80, "CRITICAL")
            .when(F.col("risk_score") >= 60, "HIGH")
            .when(F.col("risk_score") >= 35, "MEDIUM")
            .otherwise("LOW"),
        )
        .withColumn(
            "recommended_action",
            F.when(F.col("risk_score") >= 80, "BLOCK")
            .when(F.col("risk_score") >= 60, "REVIEW")
            .when(F.col("risk_score") >= 35, "STEP_UP_AUTH")
            .otherwise("ALLOW"),
        )
        .withColumn("decision_id", F.sha2(F.concat_ws("|", "transaction_id", F.lit(POLICY_VERSION)), 256))
        .withColumn("policy_version", F.lit(POLICY_VERSION))
        .withColumn("decisioned_at", F.current_timestamp())
    )

    return enriched.select(
        "decision_id", "transaction_id", "event_id", "event_timestamp", "customer_id", "account_id",
        "merchant_id", "device_id", "amount", "currency", "payment_channel", "risk_score", "risk_level",
        "recommended_action", "reason_codes", "behavioral_risk_score", "linked_customer_count",
        "resolved_identity_id", "policy_version", "decisioned_at",
    )


@dp.materialized_view(
    name="gold_investigation_queue",
    comment="Prioritized investigation-ready cases created from high-risk transaction decisions.",
    table_properties={"quality": "gold", "aegispay.domain": "case-management"},
)
def gold_investigation_queue():
    return (
        spark.read.table("gold_risk_decisions")
        .filter(F.col("recommended_action").isin("REVIEW", "BLOCK"))
        .withColumn("case_id", F.sha2(F.concat_ws("|", F.lit("CASE"), "decision_id"), 256))
        .withColumn("case_priority", F.when(F.col("recommended_action") == "BLOCK", "P1").otherwise("P2"))
        .withColumn("case_status", F.lit("OPEN"))
        .withColumn("queue_name", F.lit("FINANCIAL_CRIME_OPERATIONS"))
        .withColumn("created_at", F.current_timestamp())
        .select(
            "case_id", "decision_id", "transaction_id", "customer_id", "event_timestamp", "risk_score",
            "risk_level", "recommended_action", "reason_codes", "case_priority", "case_status", "queue_name",
            "policy_version", "created_at",
        )
    )


@dp.materialized_view(
    name="gold_access_alerts",
    comment="Prioritized insider, privileged-access, and anomalous bulk-data alerts.",
    table_properties={"quality": "gold", "aegispay.domain": "insider-access-risk"},
)
def gold_access_alerts():
    return (
        spark.read.table(SILVER_ACCESS)
        .filter(F.col("access_risk_score") >= 25)
        .withColumn("alert_id", F.sha2(F.concat_ws("|", "actor_id", "role_name", F.lit(POLICY_VERSION)), 256))
        .withColumn("risk_level", F.when(F.col("access_risk_score") >= 80, "CRITICAL").when(F.col("access_risk_score") >= 50, "HIGH").otherwise("MEDIUM"))
        .withColumn("recommended_action", F.when(F.col("access_risk_score") >= 80, "SUSPEND_AND_REVIEW").otherwise("REVIEW"))
        .withColumn("policy_version", F.lit(POLICY_VERSION))
        .withColumn("alerted_at", F.current_timestamp())
    )


@dp.materialized_view(
    name="gold_decision_metrics",
    comment="Daily decision volumes, action rates, risk distribution, and average score for operations dashboards.",
    table_properties={"quality": "gold", "aegispay.domain": "decision-monitoring"},
)
def gold_decision_metrics():
    return (
        spark.read.table("gold_risk_decisions")
        .withColumn("decision_date", F.to_date("event_timestamp"))
        .groupBy("decision_date", "risk_level", "recommended_action", "policy_version")
        .agg(
            F.count("*").alias("decision_count"),
            F.round(F.avg("risk_score"), 2).alias("average_risk_score"),
            F.round(F.sum("amount"), 2).alias("payment_amount"),
            F.countDistinct("customer_id").alias("customer_count"),
        )
        .withColumn("metrics_calculated_at", F.current_timestamp())
    )
