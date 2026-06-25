"""
Feedback Loop — identifies low-quality records and re-routes them
to MinIO reprocessing/ bucket for future re-ingestion.

Why we never delete:
  Deleting low-quality records loses information about WHY they failed.
  The reprocessing/ bucket acts as a queue AND an audit trail.
  When the source fixes the issue, these records can be re-ingested
  with the same pipeline — no code changes needed.

Flow:
  1. Query PostgreSQL for records with composite_score < threshold
  2. Find the original JSON in MinIO Bronze
  3. Copy to reprocessing/ with failure metadata attached
  4. Save report to feedback_history table
  5. Raise alert if failure rate > 50% (source critically degraded)
"""

import json
import logging
import os
import io
from datetime import datetime, timezone
from collections import Counter

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("feedback_loop")

MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET     = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
REPROCESSING_BUCKET = "reprocessing"

DB_URL = os.getenv(
    "DATABASE_URL",
    os.getenv("DATABASE_URL_LOCAL", "postgresql://airflow:airflow@localhost:5432/airflow")
)

SCORE_THRESHOLD   = float(os.getenv("FEEDBACK_SCORE_THRESHOLD", "60"))
ALERT_RATE        = float(os.getenv("FEEDBACK_ALERT_RATE", "0.5"))


# ── S3 client ─────────────────────────────────────────────────

def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def ensure_reprocessing_bucket(s3) -> None:
    """Create reprocessing bucket if it doesn't exist."""
    try:
        s3.head_bucket(Bucket=REPROCESSING_BUCKET)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=REPROCESSING_BUCKET)
            log.info("Bucket created: %s", REPROCESSING_BUCKET)
        else:
            raise


# ── PostgreSQL queries ────────────────────────────────────────

def fetch_low_quality_records(engine, run_date: str, threshold: float) -> pd.DataFrame:
    """
    Fetch records that failed evaluation.

    Why fetch ALL scores, not just composite_score?
      We want to understand WHY the record failed — was it heuristics?
      Similarity? LLM? The breakdown tells us which layer caught the issue
      and helps diagnose the root cause at the source.
    """
    sql = """
        SELECT
            record_id,
            source,
            country,
            heuristic_score,
            similarity_score,
            llm_score,
            composite_score,
            rejection_reasons,
            llm_issues
        FROM evaluation_results
        WHERE batch_date = :d
          AND composite_score < :threshold
        ORDER BY composite_score ASC
    """
    df = pd.read_sql(text(sql), engine, params={"d": run_date, "threshold": threshold})
    log.info("Found %d low-quality records (score < %s)", len(df), threshold)
    return df


def fetch_total_evaluated(engine, run_date: str) -> int:
    """Total records evaluated on this date — used to compute failure rate."""
    result = engine.execute(
        text("SELECT COUNT(*) FROM evaluation_results WHERE batch_date = :d"),
        {"d": run_date}
    ).scalar()
    return result or 0


# ── Bronze lookup ─────────────────────────────────────────────

def find_record_in_bronze(s3, record_id: str, run_date: str) -> dict | None:
    """
    Search for the original raw JSON record in Bronze bucket.

    The Bronze bucket is structured as:
      bronze/adzuna/<country>/<date>.json
      bronze/remoteok/<date>.json

    Each file is a JSON array — we search all files for the record_id.

    Why search Bronze instead of Silver?
      Bronze has the original unmodified data — useful for debugging
      why the record failed (the Silver version is already cleaned).
      Reprocessing from Bronze means the full pipeline runs again.
    """
    paginator = s3.get_paginator("list_objects_v2")
    prefix = f"adzuna/"

    for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            try:
                response = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
                records = json.loads(response["Body"].read())
                for rec in records:
                    if str(rec.get("id", "")) == str(record_id):
                        return {"source_key": key, "record": rec}
            except Exception as e:
                log.debug("Could not read %s: %s", key, e)
                continue
    return None


# ── Reprocessing upload ───────────────────────────────────────

def upload_to_reprocessing(s3, record_id: str, original: dict, failure_meta: dict, run_date: str) -> str:
    """
    Copy the original record to reprocessing/ with failure metadata attached.

    The enriched record contains:
      - All original fields from Bronze
      - _feedback_date: when this feedback run happened
      - _failure_scores: all layer scores
      - _rejection_reasons: why it failed
      - _original_bronze_key: where the original came from (audit trail)

    Key structure: reprocessing/<date>/<source>/<record_id>.json
    This makes it easy to re-ingest by date or source.
    """
    enriched = {
        **original,
        "_feedback_date":       run_date,
        "_failure_scores":      failure_meta["scores"],
        "_rejection_reasons":   failure_meta["reasons"],
        "_original_bronze_key": failure_meta.get("source_key", "unknown"),
        "_reprocessing_status": "pending",
    }

    source = original.get("_source", "unknown")
    key = f"{run_date}/{source}/{record_id}.json"

    s3.put_object(
        Bucket=REPROCESSING_BUCKET,
        Key=key,
        Body=json.dumps(enriched, ensure_ascii=False),
        ContentType="application/json",
    )
    return f"s3://{REPROCESSING_BUCKET}/{key}"


# ── Feedback report ───────────────────────────────────────────

def save_feedback_report(engine, report: dict) -> None:
    """Save the feedback run summary to PostgreSQL feedback_history."""
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO feedback_history
                    (feedback_run_date, total_evaluated, total_reprocessed,
                     avg_score_before, top_reason, reprocessing_bucket)
                VALUES
                    (:run_date, :total, :reprocessed,
                     :avg_score, :top_reason, :bucket)
            """),
            {
                "run_date":     report["run_date"],
                "total":        report["total_evaluated"],
                "reprocessed":  report["total_reprocessed"],
                "avg_score":    report["avg_score_before"],
                "top_reason":   report["top_reason"],
                "bucket":       f"s3://{REPROCESSING_BUCKET}/{report['run_date']}/",
            }
        )
    log.info("Feedback report saved to feedback_history")


# ── Entry point ───────────────────────────────────────────────

def run(run_date: str = None, score_threshold: float = None) -> dict:
    """Entry point — called by Airflow feedback_dag or directly."""
    if not run_date:
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if score_threshold is None:
        score_threshold = SCORE_THRESHOLD

    engine = create_engine(DB_URL, pool_pre_ping=True)
    s3     = get_s3()

    ensure_reprocessing_bucket(s3)

    # Step 1 — fetch low-quality records from PostgreSQL
    low_quality_df = fetch_low_quality_records(engine, run_date, score_threshold)

    if low_quality_df.empty:
        log.info("No records below threshold %.0f — nothing to reprocess", score_threshold)
        return {
            "total_evaluated":   0,
            "total_reprocessed": 0,
            "avg_score_before":  None,
            "top_reason":        None,
            "run_date":          run_date,
        }

    # Step 2 — collect all rejection reasons for the report
    all_reasons = []
    for _, row in low_quality_df.iterrows():
        reasons = row.get("rejection_reasons") or []
        if isinstance(reasons, str):
            try:
                reasons = json.loads(reasons)
            except Exception:
                reasons = [reasons]
        all_reasons.extend(reasons)

    top_reason = Counter(all_reasons).most_common(1)[0][0] if all_reasons else "unknown"

    # Step 3 — for each low-quality record, find in Bronze and copy to reprocessing
    reprocessed = 0
    for _, row in low_quality_df.iterrows():
        record_id = str(row["record_id"])

        # Build failure metadata to attach to the record
        failure_meta = {
            "scores": {
                "heuristic":  float(row.get("heuristic_score") or 0),
                "similarity": float(row.get("similarity_score") or 0),
                "llm":        float(row.get("llm_score") or 0),
                "composite":  float(row.get("composite_score") or 0),
            },
            "reasons": all_reasons,
        }

        # Find original record in Bronze
        found = find_record_in_bronze(s3, record_id, run_date)
        if not found:
            log.warning("Record %s not found in Bronze — skipping", record_id)
            continue

        failure_meta["source_key"] = found["source_key"]

        # Upload to reprocessing bucket
        dest_key = upload_to_reprocessing(
            s3, record_id, found["record"], failure_meta, run_date
        )
        log.info("Re-routed %s → %s (score=%.1f)",
                 record_id, dest_key, row["composite_score"])
        reprocessed += 1

    # Step 4 — save feedback report
    avg_score = float(low_quality_df["composite_score"].mean())
    report = {
        "run_date":          run_date,
        "total_evaluated":   len(low_quality_df),
        "total_reprocessed": reprocessed,
        "avg_score_before":  round(avg_score, 2),
        "top_reason":        top_reason,
    }
    save_feedback_report(engine, report)

    log.info("Feedback loop complete: %s", report)
    return report


if __name__ == "__main__":
    print(run())