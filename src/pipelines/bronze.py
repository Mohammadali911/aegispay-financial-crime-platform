from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_CATALOG = spark.conf.get("aegispay.source_catalog")
SOURCE_SCHEMA = spark.conf.get("aegispay.source_schema")

PAYMENT_SOURCE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.synthetic_payment_events"
CUSTOMER_SOURCE = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.synthetic_customer_changes"

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
