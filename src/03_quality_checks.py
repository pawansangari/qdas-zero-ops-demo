# Databricks notebook source
# MAGIC %md
# MAGIC ## 03 · Quality checks on the gold table
# MAGIC Fails the job if the gold table is empty or totals don't reconcile with silver.

# COMMAND ----------

dbutils.widgets.text("catalog", "cdr_dev")
dbutils.widgets.text("schema", "ai_poc")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
SILVER = f"{CATALOG}.{SCHEMA}.zeroops_claims_silver"
GOLD = f"{CATALOG}.{SCHEMA}.zeroops_provider_monthly_gold"

# COMMAND ----------

silver_total = spark.sql(f"SELECT ROUND(SUM(paid_amount), 2) FROM {SILVER}").first()[0]
gold_rows, gold_total = spark.sql(f"SELECT COUNT(*), ROUND(SUM(total_paid), 2) FROM {GOLD}").first()

assert gold_rows > 0, f"{GOLD} is empty"
assert abs(silver_total - gold_total) < 1.0, f"totals don't reconcile: silver={silver_total} gold={gold_total}"
print(f"PASS: {gold_rows:,} gold rows; paid total {gold_total:,.2f} reconciles with silver")
