# Databricks notebook source
# MAGIC %md
# MAGIC ## 01 · Load claims (bronze → silver)
# MAGIC Builds a small **synthetic** Medicare-style claims table so the demo needs no real data.
# MAGIC Output: `{catalog}.{schema}.zeroops_claims_silver`

# COMMAND ----------

dbutils.widgets.text("catalog", "cdr_dev")
dbutils.widgets.text("schema", "ai_poc")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
SILVER = f"{CATALOG}.{SCHEMA}.zeroops_claims_silver"

# COMMAND ----------

from pyspark.sql import functions as F

claims = (
    spark.range(0, 50_000)
    .withColumn("claim_id", F.concat(F.lit("CLM"), F.lpad(F.col("id").cast("string"), 7, "0")))
    .withColumn("provider_npi", (F.lit(1000000000) + (F.col("id") % 400)).cast("string"))
    .withColumn("state", F.element_at(F.array(*[F.lit(s) for s in ["MD", "VA", "PA", "NY", "TX", "CA"]]),
                                      (F.col("id") % 6 + 1).cast("int")))
    .withColumn("service_month", F.add_months(F.lit("2026-01-01").cast("date"), (F.col("id") % 9).cast("int")))
    .withColumn("paid_amount", F.round(F.rand(seed=42) * 2500 + 50, 2))
    .drop("id")
)

claims.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER)
print(f"wrote {claims.count():,} rows to {SILVER}")
display(spark.table(SILVER).limit(5))
