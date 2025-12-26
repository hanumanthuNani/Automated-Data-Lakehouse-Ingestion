variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "raw_bucket_name" {
  description = "S3 bucket name for raw data landing"
  type        = string
}

variable "cluster_name" {
  description = "Logical EMR/Spark cluster name"
  type        = string
  default     = "lakehouse-ingestion-cluster"
}

variable "ec2_key_name" {
  description = "EC2 key pair for SSH access"
  type        = string
}

variable "master_instance_type" {
  description = "EMR master instance type"
  type        = string
  default     = "m5.xlarge"
}

variable "core_instance_type" {
  description = "EMR core instance type"
  type        = string
  default     = "m5.xlarge"
}

variable "core_instance_count" {
  description = "Number of core nodes"
  type        = number
  default     = 2
}

variable "log_uri" {
  description = "S3 URI for EMR logs"
  type        = string
}

variable "subnet_id" {
  description = "Subnet for EMR cluster"
  type        = string
}
