"""
Batch ingestion job: reads raw data from S3, cleans it, and writes Parquet/ORC to HDFS partitioned by ingestion date.
"""

import argparse
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, to_timestamp

from spark.spark_utils import add_audit_columns, get_spark_session, load_config, read_raw_data, write_partitioned_data


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch ingestion for lakehouse")
    parser.add_argument("--config", default="config/ingestion.conf", help="Path to ingestion configuration file")
    return parser.parse_args()


def build_spark(conf) -> object:
    compression = conf["formats"].get("compression", "snappy")
    spark_conf = {
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.parquet.compression.codec": compression,
        "spark.sql.orc.compression.codec": compression,
    }
    return get_spark_session("batch_ingestion", spark_conf)


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    input_path = config["paths"]["s3_raw_path"]
    output_path = config["paths"]["hdfs_processed_path"]
    input_format = config["formats"].get("input_format", "json")
    output_format = config["formats"].get("output_format", "parquet")
    compression = config["formats"].get("compression", "snappy")

    spark = build_spark(config)

    # Read raw data and enforce timestamp typing for downstream pruning and deduplication.
    raw_df = read_raw_data(spark, input_path, input_format)
    cleaned = (
        raw_df.dropna(subset=["id"])
        .withColumn("last_updated", to_timestamp(col("last_updated")))
    )

    # Deduplicate by latest last_updated per business key.
    window_spec = Window.partitionBy("id").orderBy(col("last_updated").desc_nulls_last())
    deduped = cleaned.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")

    enriched = add_audit_columns(deduped)

    write_partitioned_data(
        enriched,
        output_path,
        output_format,
        partition_cols=["ingestion_date"],
        mode="overwrite",
        compression=compression,
    )


if __name__ == "__main__":
    main()
