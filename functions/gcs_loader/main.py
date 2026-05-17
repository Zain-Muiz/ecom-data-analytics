"""
GCS loader Cloud Function.

Triggered by Eventarc when an object is finalized in gs://{bucket}/incoming/.

Steps:
  1. Validate the file is a CSV in incoming/.
  2. Insert directly into raw.orders_raw using a federated CSV query.
  3. Run CALL silver.sp_run_dq_checks(load_date) — splits into silver / dead_letter.
  4. Run CALL marts.sp_refresh_marts() — rebuild marts from silver.
  5. Move the file from incoming/ to archive/YYYY/MM/DD/.

Marts refresh runs at the end of every successful load.
"""

from __future__ import annotations

import logging
import os
import re
import uuid
from datetime import datetime, timezone

from io import BytesIO
import functions_framework
from cloudevents.http.event import CloudEvent
from google.cloud import bigquery, storage
import pandas as pd

from schemas.orders import EXPECTED_COLUMNS
from schemas.orders import BQ_SCHEMA

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("gcs_loader")

PROJECT_ID = os.environ.get("GCP_PROJECT", "my-ecom-sandbox")
RAW_TABLE = os.environ.get("RAW_TABLE",  "raw.orders_raw")
DQ_PROC = os.environ.get("DQ_PROC",    "silver.sp_run_dq_checks")
MARTS_PROC = os.environ.get("MARTS_PROC", "marts.sp_refresh_marts")
LOCATION = os.environ.get("BQ_LOCATION", "US")

FNAME_RE = re.compile(r"^orders_(?P<date>\d{8})\.csv$", re.IGNORECASE)


def _parse_load_date(filename: str) -> str:
    m = FNAME_RE.match(filename)
    if m:
        d = m.group("date")
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    return datetime.now(timezone.utc).date().isoformat()


def _load_to_raw(
    bq: bigquery.Client,
    bucket_name: str,
    object_name: str,
    source_file: str,
    load_date: str,
) -> int:
    """
    Production-grade raw ingestion.

    Flow:
      GCS CSV
        -> pandas dataframe
        -> metadata enrichment
        -> schema validation
        -> BigQuery raw table

    Raw layer philosophy:
      - minimal transformation
      - preserve source fidelity
      - all source columns stored as STRING
    """

    MAX_ROWS = 5_000_000

    gcs_uri = f"gs://{bucket_name}/{object_name}"

    log.info(
        "Reading CSV into dataframe file=%s",
        gcs_uri,
    )

    # ---------------------------------------------------
    # DOWNLOAD CSV FROM GCS
    # ---------------------------------------------------
    storage_client = storage.Client(project=PROJECT_ID)

    blob = storage_client.bucket(
        bucket_name
    ).blob(object_name)

    csv_bytes = blob.download_as_bytes()

    # ---------------------------------------------------
    # READ CSV
    # ---------------------------------------------------
    df = pd.read_csv(
        BytesIO(csv_bytes),
        dtype=str,
        keep_default_na=False,
    )

    log.info(
        "Read %d rows from source file=%s",
        len(df),
        source_file,
    )

    log.info(
        "Dataframe memory usage: %.2f MB",
        df.memory_usage(deep=True).sum() / 1024 / 1024,
    )

    # ---------------------------------------------------
    # SAFETY CHECK
    # ---------------------------------------------------
    if len(df) > MAX_ROWS:
        raise ValueError(
            f"File too large for dataframe ingestion: "
            f"{len(df)} rows"
        )

    # ---------------------------------------------------
    # STANDARDIZE COLUMN NAMES
    # ---------------------------------------------------
    df.columns = [c.strip().lower() for c in df.columns]

    # ---------------------------------------------------
    # ADD METADATA COLUMNS
    # ---------------------------------------------------
    df["source_file"] = source_file
    df["load_date"] = load_date
    df["loaded_at"] = pd.Timestamp.utcnow()

    # ---------------------------------------------------
    # VALIDATE REQUIRED COLUMNS
    # ---------------------------------------------------
    missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)

    if missing_cols:
        raise ValueError(
            f"Missing expected columns: "
            f"{sorted(missing_cols)}"
        )

    # ---------------------------------------------------
    # ENFORCE COLUMN ORDER
    # ---------------------------------------------------
    df = df[EXPECTED_COLUMNS]

    log.info(
        "Validated dataframe schema successfully"
    )

    # ---------------------------------------------------
    # IDEMPOTENCY
    # ---------------------------------------------------
    log.info(
        "Deleting existing rows for source_file=%s",
        source_file,
    )

    delete_job = bq.query(
        f"""
        DELETE FROM `{PROJECT_ID}.{RAW_TABLE}`
        WHERE source_file = @source_file
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "source_file",
                    "STRING",
                    source_file,
                )
            ]
        ),
        location=LOCATION,
    )

    delete_job.result()

    log.info(
        "Deleted prior rows for source_file=%s",
        source_file,
    )

    # ---------------------------------------------------
    # LOAD DATAFRAME INTO BIGQUERY
    # ---------------------------------------------------
    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition="WRITE_APPEND",
        schema_update_options=[],
    )

    load_job = bq.load_table_from_dataframe(
        df,
        f"{PROJECT_ID}.{RAW_TABLE}",
        job_config=job_config,
        location=LOCATION,
    )

    log.info(
        "Started BigQuery load job "
        "job_id=%s destination=%s",
        load_job.job_id,
        RAW_TABLE,
    )

    load_job.result()

    rows_loaded = load_job.output_rows or len(df)

    log.info(
        "Inserted %d rows into %s from file=%s",
        rows_loaded,
        RAW_TABLE,
        source_file,
    )

    return rows_loaded


def _run_dq(bq: bigquery.Client, load_date: str) -> dict:
    sql = f"CALL `{DQ_PROC}`(DATE(@load_date));"
    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("load_date", "STRING", load_date),
        ]),
        location=LOCATION,
    )
    rows = list(job.result())
    return dict(rows[0]) if rows else {}


def _archive(bucket: storage.Bucket, blob_name: str, load_date: str) -> str:
    yyyy, mm, dd = load_date.split("-")
    new_name = f"archive/{yyyy}/{mm}/{dd}/" + os.path.basename(blob_name)
    src = bucket.blob(blob_name)
    bucket.copy_blob(src, bucket, new_name)
    src.delete()
    return new_name


def is_duplicate_file_or_event(
    bq: bigquery.Client,
    project_id: str,
    location: str,
    events_table: str,
    event_id: str,
    bucket_name: str,
    object_name: str,
    generation: str,
) -> tuple[bool, str]:
    """
    Handles BOTH:
      1. Eventarc duplicate deliveries
      2. Duplicate file uploads

    Rules:
      - Same filename => duplicate
      - Same Eventarc event => duplicate
      - Same generation => duplicate

    Returns:
      (
        is_duplicate,
        pipeline_id
      )
    """

    pipeline_id = str(uuid.uuid4())

    file_name = os.path.basename(object_name)

    dedupe_key = (
        f"{bucket_name}/{object_name}/{generation}"
    )

    sql = f"""
    MERGE `{project_id}.{events_table}` T
    USING (
        SELECT
            @pipeline_id AS pipeline_id,
            @event_id AS event_id,
            @dedupe_key AS dedupe_key,
            @bucket_name AS bucket_name,
            @object_name AS object_name,
            @generation AS generation,
            @file_name AS file_name
    ) S

    ON (
        T.event_id = S.event_id
        OR T.dedupe_key = S.dedupe_key
        OR T.file_name = S.file_name
    )

    WHEN NOT MATCHED THEN
      INSERT (
        pipeline_id,
        event_id,
        dedupe_key,
        bucket_name,
        object_name,
        generation,
        file_name,
        pipeline_status,
        processed_at
      )
      VALUES (
        S.pipeline_id,
        S.event_id,
        S.dedupe_key,
        S.bucket_name,
        S.object_name,
        S.generation,
        S.file_name,
        'IN_PROGRESS',
        CURRENT_TIMESTAMP()
      )
    """

    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "pipeline_id",
                    "STRING",
                    pipeline_id,
                ),
                bigquery.ScalarQueryParameter(
                    "event_id",
                    "STRING",
                    event_id,
                ),
                bigquery.ScalarQueryParameter(
                    "dedupe_key",
                    "STRING",
                    dedupe_key,
                ),
                bigquery.ScalarQueryParameter(
                    "bucket_name",
                    "STRING",
                    bucket_name,
                ),
                bigquery.ScalarQueryParameter(
                    "object_name",
                    "STRING",
                    object_name,
                ),
                bigquery.ScalarQueryParameter(
                    "generation",
                    "STRING",
                    generation,
                ),
                bigquery.ScalarQueryParameter(
                    "file_name",
                    "STRING",
                    file_name,
                ),
            ]
        ),
        location=location,
    )

    job.result()

    is_duplicate = (
        job.num_dml_affected_rows == 0
    )

    return is_duplicate, pipeline_id


def update_pipeline_status(
    bq: bigquery.Client,
    project_id: str,
    location: str,
    events_table: str,
    pipeline_id: str,
    status: str,
):
    """
    Update pipeline execution status.

    Example:
      SUCCESS
      FAILED
      ARCHIVED
    """

    sql = f"""
    UPDATE `{project_id}.{events_table}`
    SET
        pipeline_status = @status,
        updated_at = CURRENT_TIMESTAMP()
    WHERE pipeline_id = @pipeline_id
    """

    bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "status",
                    "STRING",
                    status,
                ),
                bigquery.ScalarQueryParameter(
                    "pipeline_id",
                    "STRING",
                    pipeline_id,
                ),
            ]
        ),
        location=location,
    ).result()

# ---------- Entry point ----------


@functions_framework.cloud_event
def on_file_landed(event: CloudEvent):
    data = event.data
    bucket_name = data["bucket"]
    name = data["name"]

    logging.info({
        "id": event["id"],
        "type": event["type"],
        "subject": event["subject"],
        "generation": event.data.get("generation"),
        "metageneration": event.data.get("metageneration"),
        "timeCreated": event.data.get("timeCreated"),
        "updated": event.data.get("updated"),
    })

    if not name.startswith("incoming/") or not name.lower().endswith(".csv"):
        log.info("Ignoring %s/%s — not a CSV in incoming/", bucket_name, name)
        return

    fname = os.path.basename(name)
    load_date = _parse_load_date(fname)
    gcs_uri = f"gs://{bucket_name}/{name}"
    log.info("Pipeline START file=%s load_date=%s", gcs_uri, load_date)

    bq = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)

    is_duplicate, pipeline_id = is_duplicate_file_or_event(
        bq=bq,
        project_id=PROJECT_ID,
        location=LOCATION,
        events_table="meta.processed_events",
        event_id=event["id"],
        bucket_name=bucket_name,
        object_name=name,
        generation=data["generation"],
    )

    if is_duplicate:
        log.warning("Duplicate file/event detected. Exiting Pipeline.")
        return

    try:
        raw_rows = _load_to_raw(bq, bucket_name, name,
                                source_file=fname, load_date=load_date)
        log.info("Loaded %d rows into raw", raw_rows)

        dq_summary = _run_dq(bq, load_date)
        log.info("DQ summary: %s", dq_summary)

        bq.query(f"CALL `{MARTS_PROC}`();", location=LOCATION).result()
        log.info("Marts refreshed")

        archived_to = _archive(bucket, name, load_date)
        log.info("Archived to gs://%s/%s", bucket_name, archived_to)

        log.info(
            "Pipeline OK file=%s raw=%d silver=%s dlq=%s",
            fname, raw_rows,
            dq_summary.get("rows_loaded_to_silver"),
            dq_summary.get("rows_to_dead_letter"),
        )

        update_pipeline_status(
            bq=bq,
            project_id=PROJECT_ID,
            location=LOCATION,
            events_table="meta.processed_events",
            pipeline_id=pipeline_id,
            status="SUCCESS",
        )
    except Exception as e:

        log.exception(
            "Pipeline FAILED file=%s error=%s",
            gcs_uri,
            str(e),
        )

        update_pipeline_status(
            bq=bq,
            project_id=PROJECT_ID,
            location=LOCATION,
            events_table="meta.processed_events",
            pipeline_id=pipeline_id,
            status="FAILED",
        )

        raise
