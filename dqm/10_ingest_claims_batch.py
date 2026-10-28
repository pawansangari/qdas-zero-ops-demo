# Databricks notebook source
# MAGIC %md
# MAGIC ## 10 · Ingest a claims batch (feeds the DQM demo)
# MAGIC Appends one **synthetic** batch to `{catalog}.{schema}.claims_feed`. Runs on a schedule, so anomaly
# MAGIC detection can learn the table's normal update cadence and volume.
# MAGIC
# MAGIC `mode` (job parameter) lets you break the feed on purpose for the demo:
# MAGIC | mode | effect |
# MAGIC |---|---|
# MAGIC | `normal` | ~2,000 complete rows per batch |
# MAGIC | `low_volume` | ~5% of normal volume → **completeness** anomaly |
# MAGIC | `null_spike` | normal volume, but ~60% of `paid_amount` and `provider_npi` are NULL → null/drift anomaly |
# MAGIC | `skip` | writes nothing (a "silent" upstream outage) → **freshness** anomaly |

# COMMAND ----------

dbutils.widgets.text("catalog", "cdr_dev")
dbutils.widgets.text("schema", "zeroops_dqm")
dbutils.widgets.text("rows", "2000")
dbutils.widgets.dropdown("mode", "normal", ["normal", "low_volume", "null_spike", "skip"])
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
ROWS, MODE = int(dbutils.widgets.get("rows")), dbutils.widgets.get("mode")
FEED = f"{CATALOG}.{SCHEMA}.claims_feed"
print(f"mode={MODE} rows={ROWS} -> {FEED}")

# COMMAND ----------

if MODE == "skip":
    dbutils.notebook.exit("skip: no batch written")

from pyspark.sql import functions as F

n = max(1, int(ROWS * (0.05 if MODE == "low_volume" else 1.0)))
null_frac = 0.6 if MODE == "null_spike" else 0.0
STATES = ["MD", "VA", "PA", "NY", "TX", "CA", "FL", "OH"]

batch = (
    spark.range(n)
    .withColumn("claim_id", F.expr("uuid()"))
    .withColumn("provider_npi", (F.lit(1000000000) + (F.rand() * 400).cast("int")).cast("string"))
    .withColumn("state", F.element_at(F.array(*[F.lit(s) for s in STATES]), (F.rand() * len(STATES) + 1).cast("int")))
    .withColumn("service_date", F.date_sub(F.current_date(), (F.rand() * 30).cast("int")))
    .withColumn("paid_amount", F.round(F.rand() * 2500 + 50, 2))
    .withColumn("claim_type", F.when(F.rand() < 0.7, "professional").otherwise("institutional"))
    .withColumn("ingest_ts", F.current_timestamp())
    .drop("id")
)
if null_frac:
    batch = (batch
             .withColumn("paid_amount", F.when(F.rand() < null_frac, None).otherwise(F.col("paid_amount")))
             .withColumn("provider_npi", F.when(F.rand() < null_frac, None).otherwise(F.col("provider_npi"))))

batch.write.mode("append").saveAsTable(FEED)
print(f"appended {n:,} rows (mode={MODE})")
