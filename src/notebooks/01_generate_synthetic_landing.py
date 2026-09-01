# Databricks notebook source
# MAGIC %md
# MAGIC # AegisPay synthetic landing data
# MAGIC Generates deterministic, privacy-safe payment and customer CDC records,
# MAGIC validates them, and writes governed Delta tables for downstream pipelines.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("payment_event_count", "1000")
dbutils.widgets.text("customer_change_count", "200")

catalog = dbutils.widgets.get("catalog")
environment = dbutils.widgets.get("environment")
payment_event_count = int(dbutils.widgets.get("payment_event_count"))
customer_change_count = int(dbutils.widgets.get("customer_change_count"))

if environment not in {"dev", "staging", "prod"}:
    raise ValueError(f"Unsupported environment: {environment}")
if payment_event_count < 100 or customer_change_count < 20:
    raise ValueError("Demonstration datasets are below the minimum validation size")

schema = f"aegispay_{environment}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

# COMMAND ----------

base_payments = spark.range(payment_event_count).withColumnRenamed("id", "event_index")

scenario = (
    F.when((F.col("event_index") % 20) == 0, F.lit("PAYMENT_FRAUD"))
    .when((F.col("event_index") % 20) == 1, F.lit("ACCOUNT_TAKEOVER"))
    .when((F.col("event_index") % 20) == 2, F.lit("MULE_NETWORK"))
    .when((F.col("event_index") % 20) == 3, F.lit("LAYERING"))
    .otherwise(F.lit("LEGITIMATE"))
)

payments = (
    base_payments
    .withColumn("scenario_label", scenario)
    .withColumn("source_sequence", F.col("event_index") + 1)
    .withColumn("event_id", F.format_string("evt_%016d", F.col("source_sequence")))
    .withColumn("transaction_id", F.format_string("txn_%016d", F.col("source_sequence")))
    .withColumn("event_timestamp", F.expr("timestamp_seconds(1767225600 + event_index * 15)"))
    .withColumn(
        "customer_slot",
        F.when(F.col("scenario_label").isin("MULE_NETWORK", "LAYERING"), F.col("event_index") % 6)
        .otherwise(F.col("event_index") % 40),
    )
    .withColumn("customer_id", F.format_string("cus_%08d", F.col("customer_slot")))
    .withColumn("account_id", F.format_string("acc_%08d", F.col("customer_slot")))
    .withColumn("merchant_id", F.format_string("mer_%08d", F.col("event_index") % 12))
    .withColumn(
        "device_slot",
        F.when(F.col("scenario_label") == "PAYMENT_FRAUD", F.lit(900))
        .when(F.col("scenario_label") == "ACCOUNT_TAKEOVER", F.lit(901))
        .when(F.col("scenario_label") == "MULE_NETWORK", F.lit(950))
        .otherwise(F.col("customer_slot")),
    )
    .withColumn("device_id", F.format_string("dev_%08d", F.col("device_slot")))
    .withColumn("ip_address", F.concat(F.lit("10.0.0."), ((F.col("device_slot") % 250) + 1).cast("string")))
    .withColumn(
        "amount",
        F.when(F.col("scenario_label") == "PAYMENT_FRAUD", F.lit(1500.00) + (F.col("event_index") % 700))
        .when(F.col("scenario_label") == "ACCOUNT_TAKEOVER", F.lit(900.00) + (F.col("event_index") % 600))
        .when(F.col("scenario_label") == "MULE_NETWORK", F.lit(500.00) + (F.col("event_index") % 350))
        .when(F.col("scenario_label") == "LAYERING", F.lit(250.00) + (F.col("event_index") % 400))
        .otherwise(F.lit(10.00) + ((F.col("event_index") * 37) % 470)),
    )
    .withColumn("amount", F.round(F.col("amount"), 2))
    .withColumn(
        "currency",
        F.element_at(
            F.array(*[F.lit(x) for x in ["CAD", "USD", "EUR", "GBP"]]),
            ((F.col("event_index") % 4) + 1).cast("int"),
        ),
    )
    .withColumn(
        "payment_channel",
        F.when(F.col("scenario_label") == "PAYMENT_FRAUD", F.lit("ECOMMERCE"))
        .when(F.col("scenario_label") == "ACCOUNT_TAKEOVER", F.lit("MOBILE"))
        .when(F.col("scenario_label").isin("MULE_NETWORK", "LAYERING"), F.lit("TRANSFER"))
        .otherwise(
            F.element_at(
                F.array(*[F.lit(x) for x in ["CARD_PRESENT", "ECOMMERCE", "MOBILE", "TRANSFER"]]),
                ((F.col("event_index") % 4) + 1).cast("int"),
            )
        ),
    )
    .withColumn("event_type", F.when(F.col("payment_channel") == "TRANSFER", F.lit("TRANSFER")).otherwise(F.lit("AUTHORISATION")))
    .withColumn("is_synthetic", F.lit(True))
    .select(
        "event_id", "event_timestamp", "transaction_id", "customer_id",
        "account_id", "merchant_id", "device_id", "ip_address", "amount",
        "currency", "payment_channel", "event_type", "source_sequence",
        "is_synthetic", "scenario_label",
    )
)

# COMMAND ----------

customer_changes = (
    spark.range(customer_change_count).withColumnRenamed("id", "change_index")
    .withColumn("source_sequence", F.col("change_index") + 1)
    .withColumn("customer_slot", F.col("change_index") % (customer_change_count // 2))
    .withColumn("change_id", F.format_string("chg_%016d", F.col("source_sequence")))
    .withColumn("change_timestamp", F.expr("timestamp_seconds(1767225600 + change_index * 60)"))
    .withColumn("operation", F.when(F.col("change_index") < customer_change_count // 2, F.lit("INSERT")).otherwise(F.lit("UPDATE")))
    .withColumn("customer_id", F.format_string("cus_%08d", F.col("customer_slot")))
    .withColumn("risk_segment", F.when((F.col("change_index") % 11) == 0, F.lit("HIGH")).otherwise(F.lit("LOW")))
    .withColumn(
        "country",
        F.element_at(
            F.array(*[F.lit(x) for x in ["CA", "US", "GB", "DE"]]),
            ((F.col("change_index") % 4) + 1).cast("int"),
        ),
    )
    .withColumn("email_hash", F.sha2(F.concat(F.lit("synthetic-email-"), F.col("customer_slot")), 256))
    .withColumn("phone_hash", F.sha2(F.concat(F.lit("synthetic-phone-"), F.col("customer_slot")), 256))
    .withColumn("is_synthetic", F.lit(True))
    .select(
        "change_id", "change_timestamp", "operation", "customer_id",
        "risk_segment", "country", "email_hash", "phone_hash",
        "source_sequence", "is_synthetic",
    )
)

# COMMAND ----------

payment_metrics = payments.agg(
    F.count("*").alias("row_count"),
    F.countDistinct("event_id").alias("distinct_event_ids"),
    F.sum(F.when(F.col("amount") <= 0, 1).otherwise(0)).alias("invalid_amounts"),
    F.sum(F.when(F.col("scenario_label").isNull(), 1).otherwise(0)).alias("missing_labels"),
).first()

if payment_metrics.row_count != payment_event_count:
    raise AssertionError("Payment row count reconciliation failed")
if payment_metrics.distinct_event_ids != payment_event_count:
    raise AssertionError("Payment event IDs are not unique")
if payment_metrics.invalid_amounts or payment_metrics.missing_labels:
    raise AssertionError("Payment data-quality validation failed")
if customer_changes.count() != customer_change_count:
    raise AssertionError("Customer CDC row count reconciliation failed")

payment_table = f"`{catalog}`.`{schema}`.`synthetic_payment_events`"
customer_table = f"`{catalog}`.`{schema}`.`synthetic_customer_changes`"

payments.write.format("delta").mode("append").saveAsTable(payment_table)
customer_changes.write.format("delta").mode("append").saveAsTable(customer_table)

summary = (
    spark.table(payment_table)
    .groupBy("scenario_label")
    .agg(F.count("*").alias("event_count"), F.round(F.sum("amount"), 2).alias("total_amount"))
    .orderBy("scenario_label")
)

display(summary)
print(
    {
        "status": "synthetic_landing_generated",
        "catalog": catalog,
        "schema": schema,
        "payment_table": payment_table,
        "customer_table": customer_table,
        "payment_event_count": payment_event_count,
        "customer_change_count": customer_change_count,
    }
)
