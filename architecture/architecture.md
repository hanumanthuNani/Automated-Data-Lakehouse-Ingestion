# Lakehouse Architecture

## Layered Design
- **Raw**: Immutable landing of source data from AWS S3. Stored as-delivered to preserve lineage and allow reprocessing.
- **Processed**: Cleaned and standardized parquet/orc datasets in HDFS. Schema enforcement, type normalization, and deduplication happen here.
- **Curated**: Optimized, analytics-friendly zone with partitioning, compression, and optional aggregates. Exposed through Hive external tables.

## Storage & Metadata
- **Storage**: S3 ingress → HDFS lakehouse tiers; Parquet/ORC for efficient IO and predicate pushdown.
- **Metadata**: Hive Metastore tracks external tables and partitions. Partition keys (e.g., `ingestion_date`) enable pruning and incremental repair.

## Compute
- **Spark**: Executes batch backfills and incremental CDC-like loads with watermarks. Uses checkpointing for stateful streaming-style increments.
- **Hive**: Provides SQL access to processed/curated layers and enforces partition-aware querying.

## Data Quality & Governance
- Schema evolution handled via Spark schema inference with explicit casts.
- Deduplication performed with window functions keyed on business identifiers and `last_updated`.
- Audit columns: `ingestion_timestamp`, `ingestion_date`, and optional source tracking.

## Reliability
- Idempotent writes via overwrite-by-partition for batch and merge/upsert semantics for incremental loads.
- Checkpoint directories isolate state per pipeline to avoid cross-contamination.

## Security & Access
- S3 bucket policies restrict write/read to pipeline roles.
- Hive permissions can be layered to expose only curated views to consumers.
