-- Hive external tables for processed and curated layers.

CREATE DATABASE IF NOT EXISTS lakehouse;
USE lakehouse;

-- Processed layer: Parquet with partition pruning on ingestion_date.
CREATE EXTERNAL TABLE IF NOT EXISTS processed_events (
  id STRING,
  name STRING,
  amount DOUBLE,
  last_updated TIMESTAMP,
  ingestion_timestamp TIMESTAMP
)
PARTITIONED BY (ingestion_date DATE)
STORED AS PARQUET
LOCATION '/data/lakehouse/processed/events'
TBLPROPERTIES (
  'parquet.compress'='SNAPPY'
);

-- Curated layer: ORC for downstream analytics with optimized compression.
CREATE EXTERNAL TABLE IF NOT EXISTS curated_events (
  id STRING,
  name STRING,
  amount DOUBLE,
  last_updated TIMESTAMP,
  ingestion_timestamp TIMESTAMP
)
PARTITIONED BY (ingestion_date DATE)
STORED AS ORC
LOCATION '/data/lakehouse/curated/events'
TBLPROPERTIES (
  'orc.compress'='ZLIB'
);

-- Register discovered partitions after ingestion.
MSCK REPAIR TABLE processed_events;
MSCK REPAIR TABLE curated_events;
