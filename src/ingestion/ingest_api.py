"""
API ingestion — fetches job data from Adzuna and uploads to MinIO Bronze layer.
Supports multi-country ingestion with retry logic and structured logging.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
import requests
from botocore.exceptions import ClientError, EndpointResolutionError
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("ingest_api")

# ── Config ────────────────────────────────────────────────────
ADZUNA_APP_ID  = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
ADZUNA_BASE    = "https://api.adzuna.com/v1/api/jobs"

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET    = os.getenv("MINIO_BUCKET_BRONZE", "bronze")

COUNTRIES = {
    "us": {"what": "data engineer", "results_per_page": 50},
    "gb": {"what": "data engineer", "results_per_page": 50},
    "br": {"what": "data engineer", "results_per_page": 50},
    "at": {"what": "data engineer", "results_per_page": 50}, # Adicionando a Áustria
}
MAX_PAGES = 4


# ── S3 / MinIO client ────────────────────────────────────────
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def ensure_bucket(s3, bucket: str) -> None:
    """Create bucket if it doesn't exist — raises on unexpected errors."""
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=bucket)
            log.info("Bucket created: %s", bucket)
        else:
            raise


def upload_to_bronze(s3, data: list[dict], key: str) -> None:
    """Upload JSON payload to MinIO Bronze bucket."""
    ensure_bucket(s3, BRONZE_BUCKET)
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False),
        ContentType="application/json",
    )
    log.info("Uploaded %d records → s3://%s/%s", len(data), BRONZE_BUCKET, key)


# ── HTTP session with retry ──────────────────────────────────
def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


# ── Adzuna fetcher ───────────────────────────────────────────
def fetch_country(session: requests.Session, country: str, params: dict) -> list[dict]:
    """Fetch all pages for one country from Adzuna API."""
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        raise EnvironmentError("ADZUNA_APP_ID and ADZUNA_APP_KEY must be set in .env")

    ingestion_ts = datetime.now(timezone.utc).isoformat()
    all_jobs: list[dict] = []

    for page in range(1, MAX_PAGES + 1):
        url = f"{ADZUNA_BASE}/{country}/search/{page}"
        payload = {
            "app_id":           ADZUNA_APP_ID,
            "app_key":          ADZUNA_APP_KEY,
            "results_per_page": params["results_per_page"],
            "what":             params["what"],
        }
        try:
            resp = session.get(url, params=payload, timeout=30)
            resp.raise_for_status()
            jobs = resp.json().get("results", [])
        except requests.exceptions.HTTPError as e:
            log.warning("HTTP %s for %s page %d — stopping", e.response.status_code, country, page)
            break
        except requests.exceptions.RequestException as e:
            log.error("Request failed for %s page %d: %s", country, page, e)
            break

        if not jobs:
            log.info("No more results for %s at page %d", country, page)
            break

        # Flatten nested objects + enrich with metadata
        for job in jobs:
            if isinstance(job.get("company"), dict):
                job["company"] = job["company"].get("display_name", "")
            if isinstance(job.get("location"), dict):
                loc = job["location"]
                area = loc.get("area", [])
                job["city"] = area[-1] if area else loc.get("display_name", "")
                job["location"] = loc.get("display_name", "")
            job["_ingested_at"] = ingestion_ts
            job["_country"]     = country
            job["_source"]      = "adzuna_api"

        all_jobs.extend(jobs)
        log.info("Fetched %d jobs from %s page %d", len(jobs), country.upper(), page)

    return all_jobs


# ── Entry point ──────────────────────────────────────────────
def run() -> dict[str, int]:
    s3      = get_s3_client()
    session = build_session()
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    summary: dict[str, int] = {}

    for country, params in COUNTRIES.items():
        log.info("Ingesting %s …", country.upper())
        jobs = fetch_country(session, country, params)

        if jobs:
            key = f"adzuna/{country}/{run_date}.json"
            upload_to_bronze(s3, jobs, key)

        summary[country] = len(jobs)
        log.info("Done %s: %d jobs", country.upper(), len(jobs))

    log.info("Ingestion complete: %s", summary)
    return summary


if __name__ == "__main__":
    result = run()
    print(result)