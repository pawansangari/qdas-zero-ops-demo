# Databricks notebook source
# MAGIC %md
# MAGIC ## 12 · Quality gate on the latest batch
# MAGIC Runs between ingest and the gold build. If the newest `claims_feed` batch is too small or has too many
# MAGIC NULLs in key columns, this task **fails the job**, so the bad batch never reaches `provider_daily_gold`.
# MAGIC Set job parameter `enforce_gate=false` to let bad batches through silently (anomaly-detection demo).

# COMMAND ----------

dbutils.widgets.text("catalog", "cdr_dev")
dbutils.widgets.text("schema", "zeroops_dqm")
dbutils.widgets.text("rows", "2000")
dbutils.widgets.dropdown("enforce_gate", "true", ["true", "false"])
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
EXPECTED_ROWS = int(dbutils.widgets.get("rows"))
ENFORCE = dbutils.widgets.get("enforce_gate") == "true"
FEED = f"{CATALOG}.{SCHEMA}.claims_feed"

MIN_ROW_RATIO = 0.5      # batch must have at least 50% of the expected rows
MAX_NULL_PCT = 0.05      # at most 5% NULLs in paid_amount / provider_npi

# COMMAND ----------

latest = spark.sql(f"""
  WITH b AS (SELECT * FROM {FEED} WHERE ingest_ts = (SELECT MAX(ingest_ts) FROM {FEED}))
  SELECT MAX(ingest_ts) AS batch_ts, COUNT(*) AS batch_rows,
         AVG(CASE WHEN paid_amount  IS NULL THEN 1 ELSE 0 END) AS pct_null_paid_amount,
         AVG(CASE WHEN provider_npi IS NULL THEN 1 ELSE 0 END) AS pct_null_provider_npi
  FROM b""").first()
print(f"latest batch {latest.batch_ts}: rows={latest.batch_rows:,} "
      f"null paid_amount={latest.pct_null_paid_amount:.1%} null provider_npi={latest.pct_null_provider_npi:.1%}")

problems = []
if latest.batch_rows < MIN_ROW_RATIO * EXPECTED_ROWS:
    problems.append(f"row count {latest.batch_rows:,} is below {MIN_ROW_RATIO:.0%} of the expected {EXPECTED_ROWS:,}")
for col in ("paid_amount", "provider_npi"):
    pct = latest[f"pct_null_{col}"]
    if pct > MAX_NULL_PCT:
        problems.append(f"{col} is {pct:.1%} NULL (limit {MAX_NULL_PCT:.0%})")

if problems and ENFORCE:
    raise ValueError(f"Quality gate failed for {FEED} batch {latest.batch_ts}: " + "; ".join(problems)
                     + ". provider_daily_gold was not rebuilt. Check the upstream source / ingest mode, "
                       "then re-run after a clean batch lands.")
print("PASS" if not problems else f"WARN (gate not enforced): {'; '.join(problems)}")
