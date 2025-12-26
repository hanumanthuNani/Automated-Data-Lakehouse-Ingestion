#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${1:-config/ingestion.conf}"

echo "Starting incremental ingestion with config: ${CONFIG_PATH}"

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --files "${CONFIG_PATH}" \
  spark/incremental_ingestion.py \
  --config "${CONFIG_PATH}"
