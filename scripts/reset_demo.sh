#!/usr/bin/env bash
# Re-introduce the demo bug on main so the job fails again (run after a demo).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
git checkout main && git pull --ff-only
sed -i.bak 's/F.sum("paid_amount")/F.sum("paid_amt")/; s/F.avg("paid_amount")/F.avg("paid_amt")/' src/02_provider_monthly_summary.py
rm -f src/02_provider_monthly_summary.py.bak
git commit -am "Rename paid_amount references in provider summary" -m "Co-authored-by: Isaac <no-reply@databricks.com>" && git push
echo "Demo reset: next run of qdas_zero_ops_demo will fail at provider_monthly_summary."
