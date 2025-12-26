# Automated Data Lakehouse Ingestion

## Project Overview
A scalable, reviewable data ingestion framework that demonstrates real-world batch and incremental pipelines landing data into a Hadoop-based lakehouse. It highlights how Spark, Hive, S3, and Terraform work together to deliver governed, performant ingestion.

## Problem Statement
Enterprises need a repeatable way to land raw S3 data into Hadoop/Hive while supporting both bulk backfills and ongoing change capture. This repository shows a reference implementation with clear patterns for orchestration, quality, schema management, and infrastructure as code.

## Architecture (Raw → Processed → Curated)
- **Raw layer**: Immutable landing zone sourced from S3.
- **Processed layer**: Cleansed, standardized Parquet/ORC stored in Hadoop with Hive external tables.
- **Curated layer**: Optimized, consumer-ready datasets with partitioning and compression for analytics.

## Tech Stack
- **Compute**: Apache Spark (batch and incremental jobs)
- **Metastore**: Apache Hive (external, partitioned tables)
- **Storage**: AWS S3 (ingress) + HDFS (lakehouse)
- **Orchestration**: Shell entrypoints (can be wired into Airflow/Oozie)
- **IaC**: Terraform for S3 + logical Hadoop/Spark cluster config

## Batch vs Incremental Ingestion
- **Batch ingestion**: Full data loads or historical backfills from S3; repartitions and writes Parquet/ORC partitioned by date.
- **Incremental ingestion**: Processes only new/updated records using a `last_updated` watermark to avoid duplicates; merges into processed tables.

## Performance Optimizations
- **Columnar formats (Parquet/ORC)**: Reduce IO, improve predicate pushdown and vectorized reads.
- **Compression**: Configurable codecs (e.g., snappy, zlib) to lower storage and network costs.
- **Partition pruning**: Tables partitioned by event or ingestion date to minimize scanned data.

## Terraform Automation
`terraform/` defines a simplified, review-friendly infrastructure: S3 bucket, IAM wiring, and logical Spark/Hadoop cluster parameters. Variables allow environment overrides; outputs surface bucket and endpoint details for pipeline wiring.

## How to Run (Conceptual)
1. Configure environment in `config/ingestion.conf` (paths, formats, compression, checkpoints).
2. Provision infra with `terraform init` and `terraform apply`.
3. Submit Spark jobs:
   - `scripts/run_batch.sh` for full loads.
   - `scripts/run_incremental.sh` for change-data loads.
4. Create/repair Hive partitions using `hive/create_external_tables.hql` and `hive/partitions.hql`.
5. Query curated tables with partition filters to leverage pruning.
