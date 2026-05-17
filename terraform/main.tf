terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google  = { source = "hashicorp/google", version = "~> 6.0" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ----------------------------------------------------------------------------
# 1. APIs
# ----------------------------------------------------------------------------
locals {
  apis = [
    "cloudfunctions.googleapis.com",
    "cloudbuild.googleapis.com",
    "run.googleapis.com",
    "eventarc.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "aiplatform.googleapis.com",
  ]
}

resource "google_project_service" "enabled" {
  for_each           = toset(local.apis)
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# ----------------------------------------------------------------------------
# 2. Service accounts
# ----------------------------------------------------------------------------
resource "google_service_account" "gcs_loader" {
  account_id   = "gcs-loader"
  display_name = "GCS Loader Function"
}

resource "google_service_account" "chatbot" {
  account_id   = "chatbot-runner"
  display_name = "ADK Chatbot (Cloud Run)"
}

# ----------------------------------------------------------------------------
# 3. GCS buckets
# ----------------------------------------------------------------------------
resource "google_storage_bucket" "landing" {
  name                        = var.landing_bucket_name
  location                    = var.bq_location
  force_destroy               = false
  uniform_bucket_level_access = true

  # Safety net: incoming/ is cleaned by loader after archive copy. This catches orphans.
  lifecycle_rule {
    condition {
      age            = 7
      matches_prefix = ["incoming/"]
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "function_src" {
  name                        = "${var.landing_bucket_name}-functions"
  location                    = var.bq_location
  force_destroy               = true
  uniform_bucket_level_access = true
}

resource "google_storage_bucket_iam_member" "loader_admin" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.gcs_loader.email}"
}

# ----------------------------------------------------------------------------
# 4. BigQuery datasets (DDL applied via scripts/init_bq.sh)
# ----------------------------------------------------------------------------
resource "google_bigquery_dataset" "raw" {
  dataset_id  = "raw"
  location    = var.bq_location
  description = "Landing zone, STRING-only schema"
}

resource "google_bigquery_dataset" "silver" {
  dataset_id  = "silver"
  location    = var.bq_location
  description = "Cleaned + DQ-checked. Source of truth."
}

resource "google_bigquery_dataset" "marts" {
  dataset_id  = "marts"
  location    = var.bq_location
  description = "Read surface for chatbot and dashboards."
}

# Loader: read+write on raw and silver
resource "google_bigquery_dataset_iam_member" "loader_raw" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.gcs_loader.email}"
}

resource "google_bigquery_dataset_iam_member" "loader_silver" {
  dataset_id = google_bigquery_dataset.silver.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.gcs_loader.email}"
}

# Loader runs scheduled query for marts refresh too
resource "google_bigquery_dataset_iam_member" "loader_marts" {
  dataset_id = google_bigquery_dataset.marts.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.gcs_loader.email}"
}

resource "google_project_iam_member" "loader_jobs" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.gcs_loader.email}"
}

# Chatbot: SELECT on marts only
resource "google_bigquery_dataset_iam_member" "chatbot_marts" {
  dataset_id = google_bigquery_dataset.marts.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.chatbot.email}"
}

resource "google_project_iam_member" "chatbot_jobs" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.chatbot.email}"
}

resource "google_project_iam_member" "chatbot_vertex" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.chatbot.email}"
}

# ----------------------------------------------------------------------------
# 5. Function source bundles
# ----------------------------------------------------------------------------
data "archive_file" "loader_src" {
  type        = "zip"
  source_dir  = "${path.module}/../functions/gcs_loader"
  output_path = "${path.module}/.build/gcs_loader.zip"
}

resource "google_storage_bucket_object" "loader_src" {
  name   = "gcs_loader-${data.archive_file.loader_src.output_md5}.zip"
  bucket = google_storage_bucket.function_src.name
  source = data.archive_file.loader_src.output_path
}

# ----------------------------------------------------------------------------
# 6. Cloud Function: gcs_loader (Gen2, Eventarc on object.finalize)
# ----------------------------------------------------------------------------
data "google_storage_project_service_account" "gcs_sa" {}

# GCS service agent needs to publish events
resource "google_project_iam_member" "gcs_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${data.google_storage_project_service_account.gcs_sa.email_address}"
}

# Loader SA needs to receive Eventarc events
resource "google_project_iam_member" "loader_eventarc_receiver" {
  project = var.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.gcs_loader.email}"
}

resource "google_cloudfunctions2_function" "gcs_loader" {
  name     = "gcs-loader"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "on_file_landed"
    source {
      storage_source {
        bucket = google_storage_bucket.function_src.name
        object = google_storage_bucket_object.loader_src.name
      }
    }
  }

  service_config {
    timeout_seconds       = 540
    available_memory      = "1Gi"
    max_instance_count    = 3
    service_account_email = google_service_account.gcs_loader.email
    environment_variables = {
      GCP_PROJECT = var.project_id
      RAW_TABLE   = "raw.orders_raw"
      DQ_PROC     = "silver.sp_run_dq_checks"
      MARTS_PROC  = "marts.sp_refresh_marts"
      BQ_LOCATION = var.bq_location
    }
  }

  event_trigger {
    trigger_region        = var.region
    event_type            = "google.cloud.storage.object.v1.finalized"
    retry_policy          = "RETRY_POLICY_RETRY"
    service_account_email = google_service_account.gcs_loader.email

    event_filters {
      attribute = "bucket"
      value     = google_storage_bucket.landing.name
    }
  }

  depends_on = [
    google_project_service.enabled,
    google_project_iam_member.gcs_pubsub_publisher,
    google_project_iam_member.loader_eventarc_receiver,
  ]
}

# ----------------------------------------------------------------------------
# 7. Outputs
# ----------------------------------------------------------------------------
output "landing_bucket" {
  value = google_storage_bucket.landing.url
}

output "chatbot_sa_email" {
  value = google_service_account.chatbot.email
}
