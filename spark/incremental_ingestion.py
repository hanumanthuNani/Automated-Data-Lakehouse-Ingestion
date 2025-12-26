"""
Incremental ingestion job: processes new/updated records using a watermark on last_updated to avoid duplicates.
"""

import argparse
from pathlib import Path
from typing import Optional

from pyspark.sql import Window
from pyspark.sql.functions import col, lit, max as spark_max, row_number, to_timestamp

from spark.spark_utils import add_audit_columns, get_spark_session, load_config, read_raw_data, write_partitioned_data


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incremental ingestion for lakehouse")
    parser.add_argument("--config", default="config/ingestion.conf", help="Path to ingestion configuration file")
    return parser.parse_args()


def build_spark(conf) -> object:
    compression = conf["formats"].get("compression", "snappy")
    spark_conf = {
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.parquet.compression.codec": compression,
        "spark.sql.orc.compression.codec": compression,
    }
    return get_spark_session("incremental_ingestion", spark_conf)


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


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    input_path = config["paths"]["s3_raw_path"]
    output_path = config["paths"]["hdfs_processed_path"]
    input_format = config["formats"].get("input_format", "json")
    output_format = config["formats"].get("output_format", "parquet")
    compression = config["formats"].get("compression", "snappy")
    watermark_path = config["incremental"].get("watermark_path", "checkpoints/last_watermark.txt")

    spark = build_spark(config)

    raw_df = read_raw_data(spark, input_path, input_format)
    staged = raw_df.withColumn("last_updated", to_timestamp(col("last_updated")))

    watermark = load_watermark(watermark_path)
    if watermark:
        staged = staged.filter(col("last_updated") > to_timestamp(lit(watermark)))

    window_spec = Window.partitionBy("id").orderBy(col("last_updated").desc_nulls_last())
    deduped = staged.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")
    enriched = add_audit_columns(deduped)

    write_partitioned_data(
        enriched,
        output_path,
        output_format,
        partition_cols=["ingestion_date"],
        mode="append",
        compression=compression,
    )

    max_last_updated = enriched.agg(spark_max("last_updated").alias("max_ts")).first()["max_ts"]
    if max_last_updated:
        persist_watermark(watermark_path, str(max_last_updated))


if __name__ == "__main__":
    main()
