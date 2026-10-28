# Databricks notebook source
# MAGIC %md
# MAGIC ## 11 · Build the provider daily gold table
# MAGIC Rebuilds `{catalog}.{schema}.provider_daily_gold` from `claims_feed` after every batch, so it sits
# MAGIC downstream in lineage. When the feed breaks, anomaly detection's root cause points upstream.

# COMMAND ----------

dbutils.widgets.text("catalog", "cdr_dev")
dbutils.widgets.text("schema", "zeroops_dqm")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
FEED, GOLD = f"{CATALOG}.{SCHEMA}.claims_feed", f"{CATALOG}.{SCHEMA}.provider_daily_gold"

# COMMAND ----------

agg = f"""
SELECT provider_npi, state, service_date,
       COUNT(*)                AS claim_count,
       ROUND(SUM(paid_amount), 2) AS total_paid,
       MAX(ingest_ts)          AS last_ingest_ts
FROM {FEED}
WHERE ingest_ts >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY provider_npi, state, service_date
"""
if spark.catalog.tableExists(GOLD):
    spark.sql(f"INSERT OVERWRITE {GOLD} {agg}")      # keeps the same Delta table (no CREATE OR REPLACE)
else:
    spark.sql(f"CREATE TABLE {GOLD} AS {agg}")
print(f"{GOLD}: {spark.table(GOLD).count():,} rows")
