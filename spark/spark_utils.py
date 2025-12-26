import configparser
from pathlib import Path
from typing import Dict, Iterable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date


def load_config(path: str) -> configparser.ConfigParser:
    config_path = Path(path)
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found or unreadable: {path}")

    config = configparser.ConfigParser()
    with config_path.open("r", encoding="utf-8") as config_file:
        config.read_file(config_file)
    return config


def get_spark_session(app_name: str, configs: Optional[Dict[str, str]] = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if configs:
        for key, value in configs.items():
            builder = builder.config(key, value)
    return builder.getOrCreate()


def read_raw_data(spark: SparkSession, path: str, fmt: str, options: Optional[Dict[str, str]] = None) -> DataFrame:
    reader = spark.read.format(fmt)
    if options:
        for key, value in options.items():
            reader = reader.option(key, value)
    return reader.load(path)


def add_audit_columns(df: DataFrame, ingestion_ts_col: str = "ingestion_timestamp", ingestion_date_col: str = "ingestion_date") -> DataFrame:
    stamped = df.withColumn(ingestion_ts_col, current_timestamp())
    return stamped.withColumn(ingestion_date_col, to_date(col(ingestion_ts_col)))


def build_spark_session(app_name: str, conf: configparser.ConfigParser) -> SparkSession:
    compression = conf.get("formats", "compression", fallback="snappy")
    spark_conf = {
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.parquet.compression.codec": compression,
        "spark.sql.orc.compression.codec": compression,
    }
    return get_spark_session(app_name, spark_conf)


def write_partitioned_data(
    df: DataFrame,
    path: str,
    fmt: str,
    partition_cols: Iterable[str],
    mode: str = "overwrite",
    compression: Optional[str] = None,
) -> None:
    writer = df.write.format(fmt).mode(mode)
    if compression:
        writer = writer.option("compression", compression)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
