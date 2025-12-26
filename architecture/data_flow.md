# Data Flow: S3 → Hadoop → Hive

## Ingestion Path
1. **Source Landing (S3)**: Raw JSON/CSV arrives in an S3 bucket organized by date.
2. **Batch / Incremental Read (Spark)**: Jobs read from S3 with schema enforcement and optional filters on `last_updated`.
3. **Transform**: Cleansing, type casting, deduplication, and enrichment (audit columns).
4. **Write to HDFS (Processed)**: Persist as Parquet/ORC partitioned by `ingestion_date` with compression.
5. **Hive External Tables**: Metastore references HDFS locations; partitions registered/added via `MSCK REPAIR` or explicit `ALTER TABLE ADD PARTITION`.
6. **Curated Access**: Downstream queries filter on partitions to leverage pruning and compression.

## Control Plane
- **Configuration**: `config/ingestion.conf` drives paths, formats, compression, and checkpoint locations.
- **Execution**: Shell scripts (`scripts/run_batch.sh`, `scripts/run_incremental.sh`) wrap `spark-submit` with consistent options.
- **Infrastructure**: Terraform provisions the S3 bucket and logical Spark/Hadoop parameters for portability.

## Partition Strategy
- Partition by `ingestion_date` (yyyy-MM-dd) to align with arrival patterns.
- For frequently queried time ranges, partition pruning minimizes scanned data and speeds queries.

## Deduplication & Watermarks
- Incremental loads track `last_updated` to read only new/changed records.
- Window-based deduplication keeps the latest record per business key (`id`, `last_updated`).
- Checkpointing ensures consistent progress across runs.
