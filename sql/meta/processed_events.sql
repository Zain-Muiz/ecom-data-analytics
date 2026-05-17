CREATE OR REPLACE TABLE `meta.processed_events`
(
  pipeline_id STRING,
  event_id STRING,
  dedupe_key STRING,
  bucket_name STRING,
  object_name STRING,
  generation STRING,
  file_name STRING,
  pipeline_status STRING,
  processed_at TIMESTAMP,
  updated_at TIMESTAMP
);