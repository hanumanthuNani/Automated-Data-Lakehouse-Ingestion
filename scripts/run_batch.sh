#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${1:-config/ingestion.conf}"

echo "Starting batch ingestion with config: ${CONFIG_PATH}"

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --files "${CONFIG_PATH}" \
  spark/batch_ingestion.py \
  --config "${CONFIG_PATH}"
