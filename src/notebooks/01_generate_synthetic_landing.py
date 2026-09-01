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
dbutils.widgets.text("authentication_event_count", "500")
dbutils.widgets.text("device_event_count", "500")
dbutils.widgets.text("access_event_count", "200")

catalog = dbutils.widgets.get("catalog")
environment = dbutils.widgets.get("environment")
payment_event_count = int(dbutils.widgets.get("payment_event_count"))
customer_change_count = int(dbutils.widgets.get("customer_change_count"))
authentication_event_count = int(dbutils.widgets.get("authentication_event_count"))
device_event_count = int(dbutils.widgets.get("device_event_count"))
access_event_count = int(dbutils.widgets.get("access_event_count"))

if environment not in {"dev", "staging", "prod"}:
    raise ValueError(f"Unsupported environment: {environment}")
if min(payment_event_count, authentication_event_count, device_event_count) < 100 or min(customer_change_count, access_event_count) < 20:
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

authentication_events = (
    spark.range(authentication_event_count).withColumnRenamed("id", "auth_index")
    .withColumn("source_sequence", F.col("auth_index") + 1)
    .withColumn("auth_event_id", F.format_string("auth_%016d", F.col("source_sequence")))
    .withColumn("event_timestamp", F.expr("timestamp_seconds(1767225600 + auth_index * 20)"))
    .withColumn("customer_slot", F.col("auth_index") % 40)
    .withColumn("customer_id", F.format_string("cus_%08d", F.col("customer_slot")))
    .withColumn("account_id", F.format_string("acc_%08d", F.col("customer_slot")))
    .withColumn("session_id", F.format_string("ses_%016d", F.col("source_sequence")))
    .withColumn(
        "scenario_label",
        F.when((F.col("auth_index") % 25) < 5, F.lit("BRUTE_FORCE"))
        .when((F.col("auth_index") % 25) == 5, F.lit("ACCOUNT_TAKEOVER"))
        .otherwise(F.lit("LEGITIMATE")),
    )
    .withColumn("device_slot", F.when(F.col("scenario_label") == "BRUTE_FORCE", F.lit(980)).when(F.col("scenario_label") == "ACCOUNT_TAKEOVER", F.lit(981)).otherwise(F.col("customer_slot")))
    .withColumn("device_id", F.format_string("dev_%08d", F.col("device_slot")))
    .withColumn("ip_address", F.concat(F.lit("10.1.0."), ((F.col("device_slot") % 250) + 1).cast("string")))
    .withColumn("country", F.when(F.col("scenario_label") == "ACCOUNT_TAKEOVER", F.lit("NL")).otherwise(F.lit("CA")))
    .withColumn("city", F.when(F.col("scenario_label") == "ACCOUNT_TAKEOVER", F.lit("Amsterdam")).otherwise(F.lit("Toronto")))
    .withColumn("auth_result", F.when(F.col("scenario_label") == "BRUTE_FORCE", F.lit("FAILURE")).otherwise(F.lit("SUCCESS")))
    .withColumn("failure_reason", F.when(F.col("auth_result") == "FAILURE", F.lit("INVALID_PASSWORD")).otherwise(F.lit(None).cast("string")))
    .withColumn("mfa_result", F.when(F.col("scenario_label") == "ACCOUNT_TAKEOVER", F.lit("BYPASSED")).when(F.col("auth_result") == "FAILURE", F.lit("NOT_CHALLENGED")).otherwise(F.lit("PASSED")))
    .withColumn("user_agent", F.when(F.col("scenario_label") == "LEGITIMATE", F.lit("SyntheticBrowser/1.0")).otherwise(F.lit("HeadlessSynthetic/1.0")))
    .withColumn("is_synthetic", F.lit(True))
    .select("auth_event_id", "event_timestamp", "customer_id", "account_id", "session_id", "device_id", "ip_address", "country", "city", "auth_result", "failure_reason", "mfa_result", "user_agent", "source_sequence", "is_synthetic", "scenario_label")
)

device_events = (
    spark.range(device_event_count).withColumnRenamed("id", "device_index")
    .withColumn("source_sequence", F.col("device_index") + 1)
    .withColumn("device_event_id", F.format_string("dvi_%016d", F.col("source_sequence")))
    .withColumn("observed_at", F.expr("timestamp_seconds(1767225600 + device_index * 30)"))
    .withColumn("customer_slot", F.col("device_index") % 40)
    .withColumn("customer_id", F.format_string("cus_%08d", F.col("customer_slot")))
    .withColumn("scenario_label", F.when((F.col("device_index") % 30) == 0, F.lit("IMPOSSIBLE_TRAVEL")).when((F.col("device_index") % 30) == 1, F.lit("ANONYMIZED_NETWORK")).otherwise(F.lit("LEGITIMATE")))
    .withColumn("device_slot", F.when(F.col("scenario_label") == "IMPOSSIBLE_TRAVEL", F.lit(990)).when(F.col("scenario_label") == "ANONYMIZED_NETWORK", F.lit(991)).otherwise(F.col("customer_slot")))
    .withColumn("device_id", F.format_string("dev_%08d", F.col("device_slot")))
    .withColumn("ip_address", F.concat(F.lit("10.2.0."), ((F.col("device_slot") % 250) + 1).cast("string")))
    .withColumn("country", F.when(F.col("scenario_label") == "IMPOSSIBLE_TRAVEL", F.lit("SG")).otherwise(F.lit("CA")))
    .withColumn("city", F.when(F.col("scenario_label") == "IMPOSSIBLE_TRAVEL", F.lit("Singapore")).otherwise(F.lit("Toronto")))
    .withColumn("latitude", F.when(F.col("scenario_label") == "IMPOSSIBLE_TRAVEL", F.lit(1.3521)).otherwise(F.lit(43.6532)))
    .withColumn("longitude", F.when(F.col("scenario_label") == "IMPOSSIBLE_TRAVEL", F.lit(103.8198)).otherwise(F.lit(-79.3832)))
    .withColumn("network_type", F.when(F.col("scenario_label") == "IMPOSSIBLE_TRAVEL", F.lit("VPN")).when(F.col("scenario_label") == "ANONYMIZED_NETWORK", F.lit("TOR")).otherwise(F.lit("RESIDENTIAL")))
    .withColumn("is_vpn", F.col("scenario_label") == "IMPOSSIBLE_TRAVEL")
    .withColumn("is_tor", F.col("scenario_label") == "ANONYMIZED_NETWORK")
    .withColumn("device_trust", F.when(F.col("scenario_label") == "ANONYMIZED_NETWORK", F.lit("BLOCKED")).when(F.col("scenario_label") == "IMPOSSIBLE_TRAVEL", F.lit("UNKNOWN")).otherwise(F.lit("TRUSTED")))
    .withColumn("is_synthetic", F.lit(True))
    .select("device_event_id", "observed_at", "customer_id", "device_id", "ip_address", "country", "city", "latitude", "longitude", "network_type", "is_vpn", "is_tor", "device_trust", "source_sequence", "is_synthetic", "scenario_label")
)

access_events = (
    spark.range(access_event_count).withColumnRenamed("id", "access_index")
    .withColumn("source_sequence", F.col("access_index") + 1)
    .withColumn("access_event_id", F.format_string("acs_%016d", F.col("source_sequence")))
    .withColumn("event_timestamp", F.expr("timestamp_seconds(1767225600 + access_index * 45)"))
    .withColumn("actor_id", F.format_string("usr_%08d", F.col("access_index") % 20))
    .withColumn("scenario_label", F.when((F.col("access_index") % 40) == 0, F.lit("PRIVILEGED_ABUSE")).when((F.col("access_index") % 40) == 1, F.lit("DATABASE_ANOMALY")).otherwise(F.lit("LEGITIMATE")))
    .withColumn("actor_type", F.lit("EMPLOYEE"))
    .withColumn("role_name", F.when(F.col("scenario_label") == "PRIVILEGED_ABUSE", F.lit("PLATFORM_ADMIN")).otherwise(F.lit("ANALYST")))
    .withColumn("source_ip", F.concat(F.lit("10.3.0."), ((F.col("access_index") % 20) + 1).cast("string")))
    .withColumn("resource_type", F.when(F.col("scenario_label") == "PRIVILEGED_ABUSE", F.lit("CUSTOMER_TABLE")).when(F.col("scenario_label") == "DATABASE_ANOMALY", F.lit("PAYMENT_TABLE")).otherwise(F.lit("CASE")))
    .withColumn("resource_name", F.concat(F.lit("synthetic_"), F.lower("resource_type")))
    .withColumn("action", F.when(F.col("scenario_label") == "PRIVILEGED_ABUSE", F.lit("EXPORT")).when(F.col("scenario_label") == "DATABASE_ANOMALY", F.lit("BULK_READ")).otherwise(F.lit("READ")))
    .withColumn("access_result", F.lit("ALLOWED"))
    .withColumn("rows_accessed", F.when(F.col("scenario_label") == "PRIVILEGED_ABUSE", F.lit(50000)).when(F.col("scenario_label") == "DATABASE_ANOMALY", F.lit(25000)).otherwise((F.col("access_index") % 25) + 1))
    .withColumn("privileged_access", F.col("scenario_label") == "PRIVILEGED_ABUSE")
    .withColumn("outside_business_hours", F.col("scenario_label") != "LEGITIMATE")
    .withColumn("is_synthetic", F.lit(True))
    .select("access_event_id", "event_timestamp", "actor_id", "actor_type", "role_name", "source_ip", "resource_type", "resource_name", "action", "access_result", "rows_accessed", "privileged_access", "outside_business_hours", "source_sequence", "is_synthetic", "scenario_label")
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
if authentication_events.count() != authentication_event_count or device_events.count() != device_event_count or access_events.count() != access_event_count:
    raise AssertionError("Security telemetry row count reconciliation failed")

payment_table = f"`{catalog}`.`{schema}`.`synthetic_payment_events`"
customer_table = f"`{catalog}`.`{schema}`.`synthetic_customer_changes`"
authentication_table = f"`{catalog}`.`{schema}`.`synthetic_authentication_events`"
device_table = f"`{catalog}`.`{schema}`.`synthetic_device_intelligence_events`"
access_table = f"`{catalog}`.`{schema}`.`synthetic_access_events`"

payments.write.format("delta").mode("append").saveAsTable(payment_table)
customer_changes.write.format("delta").mode("append").saveAsTable(customer_table)
authentication_events.write.format("delta").mode("append").saveAsTable(authentication_table)
device_events.write.format("delta").mode("append").saveAsTable(device_table)
access_events.write.format("delta").mode("append").saveAsTable(access_table)

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
        "authentication_table": authentication_table,
        "device_table": device_table,
        "access_table": access_table,
        "payment_event_count": payment_event_count,
        "customer_change_count": customer_change_count,
        "authentication_event_count": authentication_event_count,
        "device_event_count": device_event_count,
        "access_event_count": access_event_count,
    }
)
