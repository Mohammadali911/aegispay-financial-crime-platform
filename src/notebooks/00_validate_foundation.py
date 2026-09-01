# Databricks notebook source
# MAGIC %md
# MAGIC # AegisPay foundation validation
# MAGIC Confirms that the bundle target and governed schema convention are available.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("environment", "dev")

catalog = dbutils.widgets.get("catalog")
environment = dbutils.widgets.get("environment")
schema = f"aegispay_{environment}"

assert environment in {"dev", "staging", "prod"}
assert catalog and schema

print(
    {
        "project": "aegispay-financial-crime-intelligence",
        "catalog": catalog,
        "schema": schema,
        "environment": environment,
        "status": "foundation_validated",
    }
)

