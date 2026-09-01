from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_CATALOG = spark.conf.get("aegispay.source_catalog")
SOURCE_SCHEMA = spark.conf.get("aegispay.source_schema")

PAYMENT_SOURCE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.synthetic_payment_events"
CUSTOMER_SOURCE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.synthetic_customer_changes"
AUTH_SOURCE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.synthetic_authentication_events"
DEVICE_SOURCE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.synthetic_device_intelligence_events"
ACCESS_SOURCE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.synthetic_access_events"

PAYMENT_RULES = {
    "payment_event_id_required": "event_id IS NOT NULL",
    "payment_timestamp_required": "event_timestamp IS NOT NULL",
    "payment_customer_required": "customer_id IS NOT NULL",
    "payment_amount_positive": "amount > 0",
    "payment_currency_supported": "currency IN ('CAD', 'USD', 'EUR', 'GBP')",
    "payment_sequence_positive": "source_sequence > 0",
}

CUSTOMER_RULES = {
    "customer_change_id_required": "change_id IS NOT NULL",
    "customer_id_required": "customer_id IS NOT NULL",
    "customer_change_timestamp_required": "change_timestamp IS NOT NULL",
    "customer_operation_supported": "operation IN ('INSERT', 'UPDATE', 'DELETE')",
    "customer_sequence_positive": "source_sequence > 0",
}

AUTH_RULES = {
    "auth_event_id_required": "auth_event_id IS NOT NULL",
    "auth_timestamp_required": "event_timestamp IS NOT NULL",
    "auth_customer_required": "customer_id IS NOT NULL",
    "auth_result_supported": "auth_result IN ('SUCCESS', 'FAILURE')",
    "auth_sequence_positive": "source_sequence > 0",
}

DEVICE_RULES = {
    "device_event_id_required": "device_event_id IS NOT NULL",
    "device_timestamp_required": "observed_at IS NOT NULL",
    "device_customer_required": "customer_id IS NOT NULL",
    "device_network_supported": "network_type IN ('RESIDENTIAL', 'CORPORATE', 'MOBILE', 'VPN', 'TOR')",
    "device_sequence_positive": "source_sequence > 0",
}

ACCESS_RULES = {
    "access_event_id_required": "access_event_id IS NOT NULL",
    "access_timestamp_required": "event_timestamp IS NOT NULL",
    "access_actor_required": "actor_id IS NOT NULL",
    "access_result_supported": "access_result IN ('ALLOWED', 'DENIED')",
    "access_rows_nonnegative": "rows_accessed >= 0",
    "access_sequence_positive": "source_sequence > 0",
}


def invalid_condition(rules):
    return "NOT (" + " AND ".join(f"({condition})" for condition in rules.values()) + ")"


def quarantine_reasons(rules):
    return F.concat_ws(
        ",",
        *[
            F.when(~F.expr(condition), F.lit(rule_name))
            for rule_name, condition in rules.items()
        ],
    )


@dp.temporary_view(name="payment_events_staged")
def payment_events_staged():
    return (
        spark.readStream.table(PAYMENT_SOURCE)
        .select(
            F.col("event_id").cast("string"),
            F.col("event_timestamp").cast("timestamp"),
            F.col("transaction_id").cast("string"),
            F.col("customer_id").cast("string"),
            F.col("account_id").cast("string"),
            F.col("merchant_id").cast("string"),
            F.col("device_id").cast("string"),
            F.col("ip_address").cast("string"),
            F.col("amount").cast("decimal(18,2)"),
            F.col("currency").cast("string"),
            F.col("payment_channel").cast("string"),
            F.col("event_type").cast("string"),
            F.col("source_sequence").cast("long"),
            F.col("is_synthetic").cast("boolean"),
            F.col("scenario_label").cast("string"),
        )
        .withColumn("_source_table", F.lit(PAYMENT_SOURCE))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(
    name="bronze_payment_events",
    comment="Schema-enforced payment events accepted by Bronze quality controls.",
    table_properties={
        "quality": "bronze",
        "aegispay.data_classification": "synthetic-financial",
    },
)
@dp.expect_all_or_drop(PAYMENT_RULES)
def bronze_payment_events():
    return spark.readStream.table("payment_events_staged")


@dp.table(
    name="quarantine_payment_events",
    comment="Payment events rejected by Bronze quality controls with reason codes.",
    table_properties={"quality": "quarantine"},
)
@dp.expect("record_is_invalid", invalid_condition(PAYMENT_RULES))
def quarantine_payment_events():
    return (
        spark.readStream.table("payment_events_staged")
        .filter(F.expr(invalid_condition(PAYMENT_RULES)))
        .withColumn("_quarantine_reasons", quarantine_reasons(PAYMENT_RULES))
        .withColumn("_quarantined_at", F.current_timestamp())
    )


@dp.temporary_view(name="customer_changes_staged")
def customer_changes_staged():
    return (
        spark.readStream.table(CUSTOMER_SOURCE)
        .select(
            F.col("change_id").cast("string"),
            F.col("change_timestamp").cast("timestamp"),
            F.upper(F.col("operation").cast("string")).alias("operation"),
            F.col("customer_id").cast("string"),
            F.col("risk_segment").cast("string"),
            F.col("country").cast("string"),
            F.col("email_hash").cast("string"),
            F.col("phone_hash").cast("string"),
            F.col("source_sequence").cast("long"),
            F.col("is_synthetic").cast("boolean"),
        )
        .withColumn("_source_table", F.lit(CUSTOMER_SOURCE))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(
    name="bronze_customer_changes",
    comment="Schema-enforced customer CDC events accepted by Bronze quality controls.",
    table_properties={
        "quality": "bronze",
        "aegispay.data_classification": "synthetic-identity",
    },
)
@dp.expect_all_or_drop(CUSTOMER_RULES)
def bronze_customer_changes():
    return spark.readStream.table("customer_changes_staged")


@dp.table(
    name="quarantine_customer_changes",
    comment="Customer CDC events rejected by Bronze quality controls with reason codes.",
    table_properties={"quality": "quarantine"},
)
@dp.expect("record_is_invalid", invalid_condition(CUSTOMER_RULES))
def quarantine_customer_changes():
    return (
        spark.readStream.table("customer_changes_staged")
        .filter(F.expr(invalid_condition(CUSTOMER_RULES)))
        .withColumn("_quarantine_reasons", quarantine_reasons(CUSTOMER_RULES))
        .withColumn("_quarantined_at", F.current_timestamp())
    )


dp.create_streaming_table(
    name="bronze_customer_current",
    comment="Latest customer state maintained declaratively from ordered CDC events.",
    table_properties={"quality": "bronze"},
)

dp.create_auto_cdc_flow(
    target="bronze_customer_current",
    source="bronze_customer_changes",
    keys=["customer_id"],
    sequence_by=F.struct("source_sequence", "change_timestamp"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "change_id"],
    stored_as_scd_type="1",
)


@dp.temporary_view(name="authentication_events_staged")
def authentication_events_staged():
    return (
        spark.readStream.table(AUTH_SOURCE)
        .select(
            F.col("auth_event_id").cast("string"), F.col("event_timestamp").cast("timestamp"),
            F.col("customer_id").cast("string"), F.col("account_id").cast("string"),
            F.col("session_id").cast("string"), F.col("device_id").cast("string"),
            F.col("ip_address").cast("string"), F.upper("country").alias("country"),
            F.col("city").cast("string"), F.upper("auth_result").alias("auth_result"),
            F.col("failure_reason").cast("string"), F.upper("mfa_result").alias("mfa_result"),
            F.col("user_agent").cast("string"), F.col("source_sequence").cast("long"),
            F.col("is_synthetic").cast("boolean"), F.col("scenario_label").cast("string"),
        )
        .withColumn("_source_table", F.lit(AUTH_SOURCE))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(name="bronze_authentication_events", comment="Authentication and MFA telemetry accepted by Bronze controls.", table_properties={"quality": "bronze", "aegispay.data_classification": "synthetic-security"})
@dp.expect_all_or_drop(AUTH_RULES)
def bronze_authentication_events():
    return spark.readStream.table("authentication_events_staged")


@dp.table(name="quarantine_authentication_events", comment="Authentication telemetry rejected with reason codes.", table_properties={"quality": "quarantine"})
@dp.expect("record_is_invalid", invalid_condition(AUTH_RULES))
def quarantine_authentication_events():
    return spark.readStream.table("authentication_events_staged").filter(F.expr(invalid_condition(AUTH_RULES))).withColumn("_quarantine_reasons", quarantine_reasons(AUTH_RULES)).withColumn("_quarantined_at", F.current_timestamp())


@dp.temporary_view(name="device_intelligence_events_staged")
def device_intelligence_events_staged():
    return (
        spark.readStream.table(DEVICE_SOURCE)
        .select(
            F.col("device_event_id").cast("string"), F.col("observed_at").cast("timestamp"),
            F.col("customer_id").cast("string"), F.col("device_id").cast("string"),
            F.col("ip_address").cast("string"), F.upper("country").alias("country"),
            F.col("city").cast("string"), F.col("latitude").cast("double"), F.col("longitude").cast("double"),
            F.upper("network_type").alias("network_type"), F.col("is_vpn").cast("boolean"),
            F.col("is_tor").cast("boolean"), F.upper("device_trust").alias("device_trust"),
            F.col("source_sequence").cast("long"), F.col("is_synthetic").cast("boolean"),
            F.col("scenario_label").cast("string"),
        )
        .withColumn("_source_table", F.lit(DEVICE_SOURCE))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(name="bronze_device_intelligence_events", comment="Device, IP, network, and geographic telemetry accepted by Bronze controls.", table_properties={"quality": "bronze", "aegispay.data_classification": "synthetic-security"})
@dp.expect_all_or_drop(DEVICE_RULES)
def bronze_device_intelligence_events():
    return spark.readStream.table("device_intelligence_events_staged")


@dp.table(name="quarantine_device_intelligence_events", comment="Device intelligence rejected with reason codes.", table_properties={"quality": "quarantine"})
@dp.expect("record_is_invalid", invalid_condition(DEVICE_RULES))
def quarantine_device_intelligence_events():
    return spark.readStream.table("device_intelligence_events_staged").filter(F.expr(invalid_condition(DEVICE_RULES))).withColumn("_quarantine_reasons", quarantine_reasons(DEVICE_RULES)).withColumn("_quarantined_at", F.current_timestamp())


@dp.temporary_view(name="access_events_staged")
def access_events_staged():
    return (
        spark.readStream.table(ACCESS_SOURCE)
        .select(
            F.col("access_event_id").cast("string"), F.col("event_timestamp").cast("timestamp"),
            F.col("actor_id").cast("string"), F.upper("actor_type").alias("actor_type"),
            F.col("role_name").cast("string"), F.col("source_ip").cast("string"),
            F.col("resource_type").cast("string"), F.col("resource_name").cast("string"),
            F.col("action").cast("string"), F.upper("access_result").alias("access_result"),
            F.col("rows_accessed").cast("long"), F.col("privileged_access").cast("boolean"),
            F.col("outside_business_hours").cast("boolean"), F.col("source_sequence").cast("long"),
            F.col("is_synthetic").cast("boolean"), F.col("scenario_label").cast("string"),
        )
        .withColumn("_source_table", F.lit(ACCESS_SOURCE))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(name="bronze_access_events", comment="Employee, privileged, database, and API access telemetry accepted by Bronze controls.", table_properties={"quality": "bronze", "aegispay.data_classification": "synthetic-security"})
@dp.expect_all_or_drop(ACCESS_RULES)
def bronze_access_events():
    return spark.readStream.table("access_events_staged")


@dp.table(name="quarantine_access_events", comment="Access telemetry rejected with reason codes.", table_properties={"quality": "quarantine"})
@dp.expect("record_is_invalid", invalid_condition(ACCESS_RULES))
def quarantine_access_events():
    return spark.readStream.table("access_events_staged").filter(F.expr(invalid_condition(ACCESS_RULES))).withColumn("_quarantine_reasons", quarantine_reasons(ACCESS_RULES)).withColumn("_quarantined_at", F.current_timestamp())
