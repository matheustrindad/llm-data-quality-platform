"""
Data Mart loader — reads Gold Parquet from MinIO and loads into
PostgreSQL desnormalized tables for fast API queries.

Gold (MinIO)  →  load_datamart.py  →  PostgreSQL Data Mart

Tables created:
  - metrics_by_source   → consumed by NestJS GET /metrics
  - top_skills          → consumed by NestJS GET /data
  - quality_summary     → consumed by Evaluation Engine (heuristics baseline)
  - processing_summary  → audit log of each pipeline run

Design decision — why desnormalized (not Star Schema)?
  The Projeto 3 already demonstrates Star Schema modeling.
  This project uses flat analytical tables optimized for the
  specific query patterns of the NestJS API and Evaluation Engine.
  Fewer JOINs = faster API responses = better UX.
"""

import logging
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.fs as pafs
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("load_datamart")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
GOLD_BUCKET      = os.getenv("MINIO_BUCKET_GOLD", "gold")

DB_URL = os.getenv(
    "DATABASE_URL_LOCAL",
    os.getenv("DATABASE_URL", "postgresql://airflow:airflow@localhost:5432/airflow")
)

# Maps Gold Parquet paths to PostgreSQL table names
GOLD_TABLES = {
    "metrics_by_source": "metrics_by_source",
    "top_skills":        "top_skills",
    "quality_summary":   "quality_summary",
}


def get_s3_fs():
    """PyArrow S3 filesystem pointing to MinIO."""
    return pafs.S3FileSystem(
        endpoint_override=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        scheme="http",
    )


def read_gold_parquet(s3_fs, bucket: str, prefix: str) -> pd.DataFrame:
    """
    Read a Gold Parquet dataset from MinIO using PyArrow.
    PyArrow handles partitioned datasets correctly — unlike pandas.read_parquet
    which sometimes misses partition columns.
    """
    path = f"{bucket}/{prefix}"
    log.info("Reading Gold: s3://%s", path)
    dataset = pq.read_table(path, filesystem=s3_fs)
    df = dataset.to_pandas()
    log.info("Loaded %d rows from %s", len(df), path)
    return df


def ensure_tables(engine):
    """
    Create Data Mart tables if they don't exist.
    Uses CREATE TABLE IF NOT EXISTS — safe to run on every pipeline execution.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS metrics_by_source (
        id              SERIAL PRIMARY KEY,
        batch_date      DATE,
        source          VARCHAR(50),
        country         VARCHAR(10),
        posted_date     DATE,
        job_count       INT,
        company_count   INT,
        avg_salary_min  NUMERIC(12,2),
        avg_salary_max  NUMERIC(12,2),
        stddev_salary   NUMERIC(12,2),
        remote_count    INT,
        remote_pct      NUMERIC(5,1),
        loaded_at       TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS top_skills (
        id              SERIAL PRIMARY KEY,
        batch_date      DATE,
        country         VARCHAR(10),
        source          VARCHAR(50),
        skill           VARCHAR(100),
        mention_count   INT,
        loaded_at       TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS quality_summary (
        id                  SERIAL PRIMARY KEY,
        batch_date          DATE,
        total_records       INT,
        pct_has_title       NUMERIC(5,2),
        pct_has_company     NUMERIC(5,2),
        pct_has_description NUMERIC(5,2),
        pct_has_salary      NUMERIC(5,2),
        pct_has_location    NUMERIC(5,2),
        pct_senior          NUMERIC(5,2),
        pct_mid             NUMERIC(5,2),
        pct_junior          NUMERIC(5,2),
        pct_remote          NUMERIC(5,2),
        loaded_at           TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS processing_summary (
        id              SERIAL PRIMARY KEY,
        run_date        DATE,
        stage           VARCHAR(50),
        records_in      INT,
        records_out     INT,
        records_failed  INT,
        duration_secs   NUMERIC(10,2),
        status          VARCHAR(20),
        error_msg       TEXT,
        created_at      TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS evaluation_results (
        id                  SERIAL PRIMARY KEY,
        record_id           VARCHAR(100),
        batch_date          DATE,
        source              VARCHAR(50),
        country             VARCHAR(10),
        heuristic_score     NUMERIC(5,2),
        similarity_score    NUMERIC(5,2),
        llm_score           NUMERIC(5,2),
        composite_score     NUMERIC(5,2),
        rejection_reasons   TEXT[],
        llm_issues          TEXT[],
        llm_suggestions     TEXT[],
        evaluated_at        TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS feedback_history (
        id                  SERIAL PRIMARY KEY,
        feedback_run_date   DATE,
        total_evaluated     INT,
        total_reprocessed   INT,
        avg_score_before    NUMERIC(5,2),
        top_reason          VARCHAR(100),
        reprocessing_bucket VARCHAR(200),
        created_at          TIMESTAMP DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    log.info("Data Mart tables verified/created")


def load_table(engine, df: pd.DataFrame, table: str, run_date: str):
    """
    Load a DataFrame into PostgreSQL.
    Strategy: delete today's records first (idempotent), then insert fresh.
    This allows re-running the pipeline on the same day without duplicates.
    """
    df["batch_date"] = run_date

    with engine.begin() as conn:
        # Idempotent load — delete today's data before inserting
        conn.execute(
            text(f"DELETE FROM {table} WHERE batch_date = :d"),
            {"d": run_date}
        )

    df.to_sql(table, engine, if_exists="append", index=False, method="multi")
    log.info("Loaded %d rows → %s", len(df), table)


def run(run_date: str = None) -> dict:
    """Entry point — called by Airflow processing_dag or directly."""
    if not run_date:
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    engine = create_engine(DB_URL, pool_pre_ping=True)
    s3_fs  = get_s3_fs()

    # Ensure all tables exist before loading
    ensure_tables(engine)

    results = {}
    for gold_prefix, table_name in GOLD_TABLES.items():
        try:
            df = read_gold_parquet(s3_fs, GOLD_BUCKET, f"{gold_prefix}/{run_date}/")
            load_table(engine, df, table_name, run_date)
            results[table_name] = len(df)
        except Exception as e:
            log.error("Failed to load %s: %s", table_name, e)
            results[table_name] = f"ERROR: {e}"

    log.info("Data Mart load complete: %s", results)
    return results


if __name__ == "__main__":
    print(run())