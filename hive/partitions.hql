-- Partition maintenance and pruning-friendly queries.

USE lakehouse;

-- Add a specific partition if needed (example date).
ALTER TABLE processed_events ADD IF NOT EXISTS
PARTITION (ingestion_date='2025-01-01')
LOCATION '/data/lakehouse/processed/events/ingestion_date=2025-01-01';

ALTER TABLE curated_events ADD IF NOT EXISTS
PARTITION (ingestion_date='2025-01-01')
LOCATION '/data/lakehouse/curated/events/ingestion_date=2025-01-01';

-- Partition pruning example: filters on partition column avoid full scans.
SELECT id, name, amount
FROM processed_events
WHERE ingestion_date BETWEEN DATE '2025-01-01' AND DATE '2025-01-07';
