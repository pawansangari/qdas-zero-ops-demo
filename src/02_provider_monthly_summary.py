# Databricks notebook source
# MAGIC %md
# MAGIC ## 02 · Provider monthly summary (silver → gold)
# MAGIC Aggregates paid amounts per provider, state and month for the QDAS spend dashboard.
# MAGIC Output: `{catalog}.{schema}.zeroops_provider_monthly_gold`

# COMMAND ----------

dbutils.widgets.text("catalog", "cdr_dev")
dbutils.widgets.text("schema", "ai_poc")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
SILVER = f"{CATALOG}.{SCHEMA}.zeroops_claims_silver"
GOLD = f"{CATALOG}.{SCHEMA}.zeroops_provider_monthly_gold"

# COMMAND ----------

from pyspark.sql import functions as F

summary = (
    spark.table(SILVER)
    .groupBy("provider_npi", "state", "service_month")
    .agg(
        F.count("*").alias("claim_count"),
        F.sum("paid_amt").alias("total_paid"),
        F.avg("paid_amt").alias("avg_paid"),
    )
)

summary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD)
print(f"wrote {summary.count():,} rows to {GOLD}")
