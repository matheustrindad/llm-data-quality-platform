"""
Layer 1 — Heuristic Evaluation Engine.

Scores each Silver record from 0 to 100 across three dimensions:
  1. Completeness  — are required fields populated?
  2. Consistency   — are formats valid? (URLs, salary ranges)
  3. Uniqueness    — is this record a duplicate within the batch?

Final score = weighted average:
  completeness × 0.5 + consistency × 0.3 + uniqueness × 0.2

Results saved to PostgreSQL: table evaluation_results.

Why heuristics first?
  Deterministic rules run in milliseconds and catch 80% of quality
  issues. The LLM judge (Layer 3) is expensive — heuristics pre-filter
  obvious failures so the LLM only sees borderline cases.
"""

import logging
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("evaluate_heuristics")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SILVER_BUCKET    = os.getenv("MINIO_BUCKET_SILVER", "silver")

DB_URL = os.getenv(
    "DATABASE_URL",
    os.getenv("DATABASE_URL_LOCAL", "postgresql://airflow:airflow@localhost:5432/airflow")
)

# Required fields and their weights in the completeness score
REQUIRED_FIELDS = {
    "title":       0.30,   # Most important — unusable without it
    "company":     0.20,
    "description": 0.25,
    "location":    0.15,
    "country":     0.10,
}

# Score weights per dimension
WEIGHTS = {
    "completeness": 0.50,
    "consistency":  0.30,
    "uniqueness":   0.20,
}


# ── Scoring functions ─────────────────────────────────────────

def score_completeness(row: pd.Series) -> tuple[float, list[str]]:
    """
    Completeness score: weighted average of field presence.
    Returns (score 0-100, list of missing fields).

    Why weighted fields instead of simple count?
    A record missing 'title' is far worse than one missing 'location'.
    Weights reflect business impact of each missing field.
    """
    issues = []
    weighted_score = 0.0

    for field, weight in REQUIRED_FIELDS.items():
        val = row.get(field)
        if val is not None and str(val).strip() not in ("", "nan", "None"):
            weighted_score += weight
        else:
            issues.append(f"missing_{field}")

    return round(weighted_score * 100, 2), issues


def score_consistency(row: pd.Series) -> tuple[float, list[str]]:
    """
    Consistency score: validates formats and business rules.
    Returns (score 0-100, list of consistency issues).

    Rules:
    - salary_min <= salary_max (if both present)
    - salary values are positive
    - redirect_url starts with http (if present)
    - title length between 3 and 200 chars
    - country is a known 2-letter code or 'REMOTE'
    """
    issues = []
    penalties = 0.0
    checks = 0

    # Rule 1: salary range consistency
    s_min = row.get("salary_min")
    s_max = row.get("salary_max")
    if pd.notna(s_min) and pd.notna(s_max):
        checks += 1
        if float(s_min) < 0 or float(s_max) < 0:
            penalties += 1
            issues.append("negative_salary")
        elif float(s_min) > float(s_max):
            penalties += 1
            issues.append("salary_min_exceeds_max")

    # Rule 2: URL format
    url = row.get("redirect_url")
    if pd.notna(url) and str(url).strip():
        checks += 1
        if not str(url).startswith("http"):
            penalties += 0.5
            issues.append("invalid_url_format")

    # Rule 3: title length
    title = row.get("title")
    if pd.notna(title) and str(title).strip():
        checks += 1
        tlen = len(str(title).strip())
        if tlen < 3:
            penalties += 1
            issues.append("title_too_short")
        elif tlen > 200:
            penalties += 0.3
            issues.append("title_too_long")

    # Rule 4: country code
    country = row.get("country")
    valid_countries = {"US", "GB", "BR", "AT", "REMOTE", "XX"}
    if pd.notna(country) and str(country).strip():
        checks += 1
        if str(country).upper() not in valid_countries:
            penalties += 0.5
            issues.append(f"unknown_country_{country}")

    if checks == 0:
        return 100.0, []

    score = max(0.0, (1 - penalties / checks) * 100)
    return round(score, 2), issues


def score_uniqueness(record_id: str, seen_ids: set) -> tuple[float, list[str]]:
    """
    Uniqueness score: checks if this record ID was already seen in the batch.
    Returns (score 0-100, list of issues).

    100 = unique record
    0   = duplicate within this batch

    Note: this is intra-batch deduplication. Inter-batch deduplication
    (across multiple runs) is handled by PySpark window functions in Silver.
    """
    if record_id in seen_ids:
        return 0.0, ["duplicate_id_in_batch"]
    return 100.0, []


def compute_composite(completeness: float, consistency: float, uniqueness: float) -> float:
    """Weighted average of the three dimension scores."""
    return round(
        completeness * WEIGHTS["completeness"] +
        consistency  * WEIGHTS["consistency"] +
        uniqueness   * WEIGHTS["uniqueness"],
        2
    )


# ── Data loading ──────────────────────────────────────────────

def load_silver(run_date: str) -> pd.DataFrame:
    """Read Silver Parquet from MinIO using boto3 + pyarrow."""
    import boto3
    import io

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    prefix = f"jobs/{run_date}/"
    log.info("Reading Silver: s3://%s/%s", SILVER_BUCKET, prefix)

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=SILVER_BUCKET, Prefix=prefix)

    frames = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            response = s3.get_object(Bucket=SILVER_BUCKET, Key=key)
            buf = io.BytesIO(response["Body"].read())
            frames.append(pd.read_parquet(buf))

    if not frames:
        raise FileNotFoundError(f"No Parquet files found in s3://{SILVER_BUCKET}/{prefix}")

    df = pd.concat(frames, ignore_index=True)
    log.info("Loaded %d records from Silver", len(df))
    return df


# ── PostgreSQL ────────────────────────────────────────────────

def save_results(engine, results: list[dict], run_date: str):
    """
    Save evaluation results to PostgreSQL.
    Uses idempotent load — deletes today's heuristic results before inserting.
    """
    if not results:
        log.warning("No results to save")
        return

    df = pd.DataFrame(results)

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM evaluation_results WHERE batch_date = :d AND heuristic_score IS NOT NULL"),
            {"d": run_date}
        )

    df.to_sql("evaluation_results", engine, if_exists="append", index=False, method="multi")
    log.info("Saved %d evaluation results to PostgreSQL", len(df))


# ── Entry point ───────────────────────────────────────────────

def run(run_date: str = None) -> dict:
    """Entry point — called by Airflow evaluation_dag or directly."""
    if not run_date:
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    df = load_silver(run_date)
    engine = create_engine(DB_URL, pool_pre_ping=True)

    results = []
    seen_ids = set()

    score_distribution = {"low": 0, "medium": 0, "high": 0}

    for _, row in df.iterrows():
        record_id = str(row.get("id", ""))

        # Score each dimension
        completeness_score, completeness_issues = score_completeness(row)
        consistency_score,  consistency_issues  = score_consistency(row)
        uniqueness_score,   uniqueness_issues   = score_uniqueness(record_id, seen_ids)

        seen_ids.add(record_id)

        # Composite score
        composite = compute_composite(completeness_score, consistency_score, uniqueness_score)

        # Track distribution
        if composite < 50:
            score_distribution["low"] += 1
        elif composite < 80:
            score_distribution["medium"] += 1
        else:
            score_distribution["high"] += 1

        all_issues = completeness_issues + consistency_issues + uniqueness_issues

        results.append({
            "record_id":        record_id,
            "batch_date":       run_date,
            "source":           str(row.get("_source", "unknown")),
            "country":          str(row.get("country", "XX")),
            "heuristic_score":  composite,
            "similarity_score": None,   # Filled by Layer 2
            "llm_score":        None,   # Filled by Layer 3
            "composite_score":  composite,  # Updated after all layers
            "rejection_reasons": all_issues if all_issues else [],
            "llm_issues":       [],
            "llm_suggestions":  [],
        })

    save_results(engine, results, run_date)

    avg_score = sum(r["heuristic_score"] for r in results) / len(results) if results else 0
    below_threshold = sum(1 for r in results if r["heuristic_score"] < 60)

    summary = {
        "scored":          len(results),
        "avg_score":       round(avg_score, 2),
        "below_threshold": below_threshold,
        "distribution":    score_distribution,
        "date":            run_date,
    }
    log.info("Heuristic evaluation complete: %s", summary)
    return summary


if __name__ == "__main__":
    print(run())