from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_CATALOG = spark.conf.get("aegispay.source_catalog")
SOURCE_SCHEMA = spark.conf.get("aegispay.source_schema")

BRONZE_PAYMENTS = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.bronze_payment_events"
BRONZE_CUSTOMERS = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.bronze_customer_current"

SILVER_PAYMENT_RULES = {
    "event_identity_complete": "event_id IS NOT NULL AND transaction_id IS NOT NULL",
    "payment_parties_complete": "customer_id IS NOT NULL AND account_id IS NOT NULL",
    "payment_timestamp_complete": "event_timestamp IS NOT NULL",
    "payment_amount_valid": "amount > 0",
}


@dp.table(
    name="silver_payments",
    comment="Validated payment events deduplicated by the contracted event identifier.",
    table_properties={
        "quality": "silver",
        "aegispay.processing_guarantee": "watermark-deduplicated",
    },
)
@dp.expect_all_or_fail(SILVER_PAYMENT_RULES)
def silver_payments():
    return (
        spark.readStream
        .option("withEventTimeOrder", "true")
        .table(BRONZE_PAYMENTS)
        .withWatermark("event_timestamp", "1 day")
        .dropDuplicatesWithinWatermark(["event_id"])
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("_conformed_at", F.current_timestamp())
    )


@dp.materialized_view(
    name="silver_customers",
    comment="Conformed current customers with deterministic, privacy-safe identity resolution.",
    table_properties={
        "quality": "silver",
        "aegispay.identity_resolution": "deterministic-hashed-identifiers",
    },
)
@dp.expect_or_fail("resolved_customer_required", "resolved_identity_id IS NOT NULL")
def silver_customers():
    return (
        spark.read.table(BRONZE_CUSTOMERS)
        .withColumn("country", F.upper(F.trim("country")))
        .withColumn("risk_segment", F.upper(F.trim("risk_segment")))
        .withColumn(
            "resolved_identity_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.coalesce("email_hash", F.lit("missing-email")),
                    F.coalesce("phone_hash", F.lit("missing-phone")),
                ),
                256,
            ),
        )
        .withColumn(
            "identity_match_method",
            F.when(
                F.col("email_hash").isNotNull() & F.col("phone_hash").isNotNull(),
                F.lit("EMAIL_AND_PHONE_HASH"),
            ).otherwise(F.lit("PARTIAL_HASH_IDENTITY")),
        )
    )


def network_edge_frame(payments, edge_type, source_type, source_column, target_type, target_column):
    return payments.select(
        F.lit(edge_type).alias("edge_type"),
        F.lit(source_type).alias("source_type"),
        F.col(source_column).alias("source_id"),
        F.lit(target_type).alias("target_type"),
        F.col(target_column).alias("target_id"),
        "transaction_id",
        "event_timestamp",
        "amount",
        "currency",
        "scenario_label",
    )


@dp.materialized_view(
    name="silver_transaction_edges",
    comment="Aggregated customer, account, merchant, and device relationships for graph analytics.",
    table_properties={
        "quality": "silver",
        "aegispay.model": "transaction-network",
    },
)
def silver_transaction_edges():
    payments = spark.read.table("silver_payments")
    edges = (
        network_edge_frame(payments, "OWNS", "CUSTOMER", "customer_id", "ACCOUNT", "account_id")
        .unionByName(
            network_edge_frame(
                payments,
                "PAYS",
                "ACCOUNT",
                "account_id",
                "MERCHANT",
                "merchant_id",
            )
        )
        .unionByName(
            network_edge_frame(
                payments,
                "USES",
                "CUSTOMER",
                "customer_id",
                "DEVICE",
                "device_id",
            )
        )
    )

    return (
        edges.groupBy(
            "edge_type",
            "source_type",
            "source_id",
            "target_type",
            "target_id",
            "currency",
        )
        .agg(
            F.countDistinct("transaction_id").alias("transaction_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.min("event_timestamp").alias("first_seen_at"),
            F.max("event_timestamp").alias("last_seen_at"),
            F.sum(
                F.when(F.col("scenario_label") != "LEGITIMATE", 1).otherwise(0)
            ).alias("labeled_risk_event_count"),
        )
    )
