"""
Incremental ingestion job: processes new/updated records using a watermark on last_updated to avoid duplicates.
"""

import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

from pyspark.sql import Window
from pyspark.sql.functions import col, lit, max as spark_max, row_number, to_timestamp

from spark.spark_utils import (
    add_audit_columns,
    build_spark_session,
    load_config,
    read_raw_data,
    write_partitioned_data,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incremental ingestion for lakehouse")
    parser.add_argument("--config", default="config/ingestion.conf", help="Path to ingestion configuration file")
    return parser.parse_args()


def load_watermark(path: Optional[str]) -> Optional[str]:
    if path and Path(path).exists():
        return Path(path).read_text(encoding="utf-8").strip() or None
    return None


def persist_watermark(path: Optional[str], value: str) -> None:
    if not path:
        return
    watermark_file = Path(path)
    watermark_file.parent.mkdir(parents=True, exist_ok=True)
    watermark_file.write_text(value, encoding="utf-8")


def resolve_max_timestamp(row):
    if row and row["max_ts"] is not None:
        return row["max_ts"]
    return datetime.utcnow()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    input_path = config.get("paths", "s3_raw_path", fallback=None)
    output_path = config.get("paths", "hdfs_processed_path", fallback=None)
    if not input_path or not output_path:
        raise ValueError("Both paths.s3_raw_path and paths.hdfs_processed_path must be configured")

    input_format = config.get("formats", "input_format", fallback="json")
    output_format = config.get("formats", "output_format", fallback="parquet")
    compression = config.get("formats", "compression", fallback="snappy")
    watermark_path = config.get("incremental", "watermark_path", fallback="checkpoints/last_watermark.txt")
    business_key = config.get("schema", "business_key", fallback="id")

    spark = build_spark_session("incremental_ingestion", config)

    raw_df = read_raw_data(spark, input_path, input_format)
    staged = raw_df.dropna(subset=[business_key]).withColumn("last_updated", to_timestamp(col("last_updated")))

    watermark = load_watermark(watermark_path)
    if watermark:
        staged = staged.filter(col("last_updated") >= to_timestamp(lit(watermark)))

    window_spec = Window.partitionBy(business_key).orderBy(col("last_updated").desc_nulls_last())
    deduped = staged.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")
    enriched = add_audit_columns(deduped)
    if enriched.isEmpty():
        print("No new incremental data to process; exiting.")
        return

    enriched = enriched.cache()

    agg_row = enriched.agg(spark_max("last_updated").alias("max_ts")).first()

    max_last_updated = resolve_max_timestamp(agg_row)

    write_partitioned_data(
        enriched,
        output_path,
        output_format,
        partition_cols=["ingestion_date"],
        mode="append",
        compression=compression,
    )

    enriched.unpersist()
    persist_watermark(watermark_path, str(max_last_updated))


if __name__ == "__main__":
    main()
