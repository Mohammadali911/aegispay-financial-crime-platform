from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_CATALOG = spark.conf.get("aegispay.source_catalog")
SOURCE_SCHEMA = spark.conf.get("aegispay.source_schema")

BRONZE_PAYMENTS = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.bronze_payment_events"
BRONZE_CUSTOMERS = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.bronze_customer_current"
BRONZE_AUTH = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.bronze_authentication_events"
BRONZE_DEVICE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.bronze_device_intelligence_events"
BRONZE_ACCESS = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.bronze_access_events"

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


@dp.table(
    name="silver_authentication_events",
    comment="Deduplicated authentication and MFA activity for behavioral detection.",
    table_properties={"quality": "silver", "aegispay.domain": "authentication"},
)
@dp.expect_or_fail("authentication_identity_complete", "auth_event_id IS NOT NULL AND customer_id IS NOT NULL")
def silver_authentication_events():
    return (
        spark.readStream.table(BRONZE_AUTH)
        .withWatermark("event_timestamp", "1 day")
        .dropDuplicatesWithinWatermark(["auth_event_id"])
        .withColumn("is_failed_login", F.col("auth_result") == "FAILURE")
        .withColumn("is_mfa_bypass", F.col("mfa_result") == "BYPASSED")
        .withColumn("is_headless_client", F.lower("user_agent").contains("headless"))
        .withColumn("_conformed_at", F.current_timestamp())
    )


@dp.table(
    name="silver_device_intelligence_events",
    comment="Deduplicated device, IP, network, and geographic observations.",
    table_properties={"quality": "silver", "aegispay.domain": "device-intelligence"},
)
@dp.expect_or_fail("device_identity_complete", "device_event_id IS NOT NULL AND customer_id IS NOT NULL")
def silver_device_intelligence_events():
    return (
        spark.readStream.table(BRONZE_DEVICE)
        .withWatermark("observed_at", "1 day")
        .dropDuplicatesWithinWatermark(["device_event_id"])
        .withColumn("is_anonymized_network", F.col("is_vpn") | F.col("is_tor"))
        .withColumn("is_untrusted_device", F.col("device_trust") != "TRUSTED")
        .withColumn("_conformed_at", F.current_timestamp())
    )


@dp.table(
    name="silver_access_events",
    comment="Deduplicated employee, privileged, database, and API access audit events.",
    table_properties={"quality": "silver", "aegispay.domain": "access-audit"},
)
@dp.expect_or_fail("access_identity_complete", "access_event_id IS NOT NULL AND actor_id IS NOT NULL")
def silver_access_events():
    return (
        spark.readStream.table(BRONZE_ACCESS)
        .withWatermark("event_timestamp", "1 day")
        .dropDuplicatesWithinWatermark(["access_event_id"])
        .withColumn("is_bulk_access", F.col("rows_accessed") >= 10000)
        .withColumn("is_privileged_after_hours", F.col("privileged_access") & F.col("outside_business_hours"))
        .withColumn("_conformed_at", F.current_timestamp())
    )


@dp.materialized_view(
    name="silver_customer_behavioral_features",
    comment="Explainable customer authentication, device, IP, and geographic risk features.",
    table_properties={"quality": "silver", "aegispay.feature_group": "behavioral-risk"},
)
def silver_customer_behavioral_features():
    auth = (
        spark.read.table("silver_authentication_events")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("authentication_count"),
            F.sum(F.col("is_failed_login").cast("long")).alias("failed_login_count"),
            F.sum(F.col("is_mfa_bypass").cast("long")).alias("mfa_bypass_count"),
            F.sum(F.col("is_headless_client").cast("long")).alias("headless_client_count"),
            F.countDistinct("device_id").alias("authentication_device_count"),
            F.countDistinct("ip_address").alias("authentication_ip_count"),
            F.max("event_timestamp").alias("last_authentication_at"),
        )
    )
    devices = (
        spark.read.table("silver_device_intelligence_events")
        .groupBy("customer_id")
        .agg(
            F.countDistinct("device_id").alias("observed_device_count"),
            F.countDistinct("country").alias("observed_country_count"),
            F.sum(F.col("is_anonymized_network").cast("long")).alias("anonymized_network_count"),
            F.sum(F.col("is_untrusted_device").cast("long")).alias("untrusted_device_count"),
            F.sum((F.upper(F.col("country")) != "CA").cast("long")).alias("impossible_travel_count"),
            F.max("observed_at").alias("last_device_observation_at"),
        )
    )
    return (
        auth.join(devices, "customer_id", "full")
        .fillna(0, subset=[
            "authentication_count", "failed_login_count", "mfa_bypass_count", "headless_client_count",
            "authentication_device_count", "authentication_ip_count", "observed_device_count",
            "observed_country_count", "anonymized_network_count", "untrusted_device_count",
            "impossible_travel_count",
        ])
        .withColumn(
            "behavioral_risk_score",
            F.least(
                F.lit(100),
                F.col("failed_login_count") * 4
                + F.col("mfa_bypass_count") * 30
                + F.col("headless_client_count") * 10
                + F.col("anonymized_network_count") * 15
                + F.col("untrusted_device_count") * 10
                + F.col("impossible_travel_count") * 35,
            ),
        )
        .withColumn(
            "reason_codes",
            F.array_compact(F.array(
                F.when(F.col("failed_login_count") >= 3, F.lit("REPEATED_LOGIN_FAILURES")),
                F.when(F.col("mfa_bypass_count") > 0, F.lit("MFA_BYPASS")),
                F.when(F.col("headless_client_count") > 0, F.lit("HEADLESS_CLIENT")),
                F.when(F.col("anonymized_network_count") > 0, F.lit("VPN_OR_TOR")),
                F.when(F.col("untrusted_device_count") > 0, F.lit("UNTRUSTED_DEVICE")),
                F.when(F.col("impossible_travel_count") > 0, F.lit("IMPOSSIBLE_TRAVEL")),
            )),
        )
        .withColumn("feature_calculated_at", F.current_timestamp())
    )


@dp.materialized_view(
    name="silver_access_risk_signals",
    comment="Explainable privileged-access and database/API anomaly signals.",
    table_properties={"quality": "silver", "aegispay.feature_group": "insider-access-risk"},
)
def silver_access_risk_signals():
    return (
        spark.read.table("silver_access_events")
        .groupBy("actor_id", "role_name")
        .agg(
            F.count("*").alias("access_event_count"),
            F.sum(F.col("is_bulk_access").cast("long")).alias("bulk_access_count"),
            F.sum(F.col("is_privileged_after_hours").cast("long")).alias("privileged_after_hours_count"),
            F.max("rows_accessed").alias("maximum_rows_accessed"),
            F.max("event_timestamp").alias("last_access_at"),
        )
        .withColumn(
            "access_risk_score",
            F.least(F.lit(100), F.col("bulk_access_count") * 25 + F.col("privileged_after_hours_count") * 40),
        )
        .withColumn(
            "reason_codes",
            F.array_compact(F.array(
                F.when(F.col("bulk_access_count") > 0, F.lit("BULK_DATA_ACCESS")),
                F.when(F.col("privileged_after_hours_count") > 0, F.lit("PRIVILEGED_AFTER_HOURS")),
            )),
        )
        .withColumn("feature_calculated_at", F.current_timestamp())
    )
