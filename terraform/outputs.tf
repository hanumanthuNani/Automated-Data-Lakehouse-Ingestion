output "raw_bucket_name" {
  description = "Name of the S3 bucket used for raw data landing"
  value       = aws_s3_bucket.raw_data.bucket
}

output "emr_cluster_id" {
  description = "ID of the EMR cluster running Spark/Hive"
  value       = aws_emr_cluster.spark.id
}
