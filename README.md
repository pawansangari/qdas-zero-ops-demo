# QDAS Zero Ops demo pipeline

A small QDAS-style claims pipeline on **synthetic data**, used to demo Genie Zero Ops:
a job fails, Genie diagnoses it, proposes a fix, and opens a pull request here.

| Task | Notebook | Output |
|---|---|---|
| load_claims | `src/01_load_claims.py` | `cdr_dev.ai_poc.zeroops_claims_silver` |
| provider_monthly_summary | `src/02_provider_monthly_summary.py` | `cdr_dev.ai_poc.zeroops_provider_monthly_gold` |
| quality_checks | `src/03_quality_checks.py` | pass/fail reconciliation |

The Databricks job `qdas_zero_ops_demo` runs these notebooks directly from the `main` branch of this repo
(job Git source), so a merged pull request is picked up on the next run with no redeploy.

To reset the demo after a run, see `scripts/reset_demo.sh`.

## Table health demo (data quality monitoring)

`dqm/` holds a second, scheduled pipeline used to demo Genie Zero Ops on **table health**:

| Notebook | What it does |
|---|---|
| `dqm/10_ingest_claims_batch.py` | Appends a synthetic claims batch to `cdr_dev.zeroops_dqm.claims_feed` every 30 min |
| `dqm/11_build_provider_daily.py` | Rebuilds the downstream `cdr_dev.zeroops_dqm.provider_daily_gold` |

Job `qdas_zero_ops_dqm_feed` runs these. Anomaly detection is enabled on schema `cdr_dev.zeroops_dqm`, and
`claims_feed` has a time-series data-profiling monitor (hourly windows, sliced by `state` and `claim_type`).
The job parameter `mode` breaks the feed on purpose: `low_volume` (completeness), `null_spike` (nulls/drift),
`skip` (freshness).
