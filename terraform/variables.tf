variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "Region for Cloud Functions, Run, Eventarc"
  type        = string
  default     = "us-central1"
}

variable "bq_location" {
  description = "BigQuery location (multi-region or region)"
  type        = string
  default     = "US"
}

variable "landing_bucket_name" {
  description = "Globally unique GCS bucket name for landing CSVs"
  type        = string
}
