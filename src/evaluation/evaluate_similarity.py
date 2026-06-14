"""
Layer 2 — Semantic Similarity Evaluation.

Detects semantically duplicate records using sentence embeddings.
Two records are "semantic duplicates" if their title+description
embeddings have cosine similarity above the threshold (default 0.92).

Why this matters:
  Exact-match deduplication (Layer 1 / PySpark window) catches records
  with the same ID. But the same job posting often appears with slightly
  different titles or descriptions across sources:
    "Senior Data Engineer" vs "Sr. Data Engineer - Remote"
  These are the same job but pass exact-match dedup. Semantic similarity
  catches them by comparing meaning, not text.

Strategy:
  1. Generate embeddings for title + description (concatenated)
  2. Compute pairwise cosine similarity matrix
  3. Flag records above threshold as semantic duplicates
  4. Update similarity_score in evaluation_results table

Score interpretation:
  100 = unique record (no similar records found)
  0   = exact semantic duplicate
  50  = similar but not duplicate (below threshold)

Model: paraphrase-MiniLM-L6-v2
  - Small (80MB), fast, runs on CPU
  - Good enough for job title/description similarity
  - No GPU required
"""

import logging
import os
import io
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("evaluate_similarity")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SILVER_BUCKET    = os.getenv("MINIO_BUCKET_SILVER", "silver")

DB_URL = os.getenv(
    "DATABASE_URL",
    os.getenv("DATABASE_URL_LOCAL", "postgresql://airflow:airflow@localhost:5432/airflow")
)

# Cosine similarity above this threshold = semantic duplicate
SIMILARITY_THRESHOLD = float(os.getenv("SIMILARITY_THRESHOLD", "0.92"))

# Max records to embed — keeps memory under control on local machines
# For 416 records, the similarity matrix is 416×416 = ~1.4M comparisons
MAX_RECORDS = int(os.getenv("SIMILARITY_MAX_RECORDS", "500"))


# ── Data loading ──────────────────────────────────────────────

def load_silver(run_date: str) -> pd.DataFrame:
    """Read Silver Parquet from MinIO using boto3."""
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
    frames = []
    for page in paginator.paginate(Bucket=SILVER_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                buf = io.BytesIO(s3.get_object(
                    Bucket=SILVER_BUCKET, Key=obj["Key"]
                )["Body"].read())
                frames.append(pd.read_parquet(buf))

    if not frames:
        raise FileNotFoundError(f"No Parquet in s3://{SILVER_BUCKET}/{prefix}")

    df = pd.concat(frames, ignore_index=True)
    log.info("Loaded %d records", len(df))
    return df


# ── Embedding ─────────────────────────────────────────────────

def build_texts(df: pd.DataFrame) -> list[str]:
    """
    Concatenate title + description for embedding.
    Why concatenate?
      Title alone misses context ("Engineer" could be civil or data).
      Description alone is too long and noisy.
      Title (30%) + description (70%) gives the best signal.
    Truncate description to 200 chars to keep embeddings fast.
    """
    texts = []
    for _, row in df.iterrows():
        title = str(row.get("title") or "").strip()
        desc  = str(row.get("description") or "").strip()[:200]
        texts.append(f"{title}. {desc}".strip())
    return texts


def generate_embeddings(texts: list[str]) -> np.ndarray:
    """
    Generate sentence embeddings using sentence-transformers.

    Why paraphrase-MiniLM-L6-v2?
      - 80MB model — downloads once, cached locally
      - 384-dimension embeddings — fast cosine similarity
      - Trained on paraphrase detection — perfect for our use case
      - Runs on CPU — no GPU required

    Returns: numpy array of shape (n_records, 384)
    """
    from sentence_transformers import SentenceTransformer

    log.info("Loading sentence-transformers model...")
    model = SentenceTransformer("paraphrase-MiniLM-L6-v2")

    log.info("Generating embeddings for %d texts...", len(texts))
    embeddings = model.encode(
        texts,
        batch_size=32,
        show_progress_bar=False,
        convert_to_numpy=True,
    )
    log.info("Embeddings shape: %s", embeddings.shape)
    return embeddings


def cosine_similarity_matrix(embeddings: np.ndarray) -> np.ndarray:
    """
    Compute pairwise cosine similarity matrix.

    Cosine similarity = dot product of unit vectors.
    Values range from -1 (opposite) to 1 (identical).
    For job descriptions, values above 0.92 indicate near-duplicates.

    Why not Euclidean distance?
      Cosine similarity is invariant to vector magnitude — it measures
      the angle between vectors, not their length. This makes it robust
      to different description lengths.
    """
    # Normalize to unit vectors
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1, norms)  # avoid division by zero
    unit = embeddings / norms

    # Matrix multiplication = all pairwise dot products
    sim_matrix = np.dot(unit, unit.T)
    return sim_matrix


# ── Scoring ───────────────────────────────────────────────────

def compute_similarity_scores(
    df: pd.DataFrame,
    sim_matrix: np.ndarray,
    threshold: float,
) -> tuple[list[float], list[str], int]:
    """
    For each record, find its most similar neighbor (excluding itself).
    Returns (scores, duplicate_ids, flagged_count).

    Score logic:
      - max_sim < threshold → score = 100 (unique)
      - max_sim >= threshold → score = (1 - max_sim) * 100
        A record with similarity 0.97 gets score = (1-0.97)*100 = 3
        A record with similarity 0.92 (threshold) gets score = 8
    """
    n = len(df)
    scores = []
    flagged_ids = []
    flagged_count = 0

    for i in range(n):
        # Get similarities to all other records (exclude self = diagonal)
        row_sim = sim_matrix[i].copy()
        row_sim[i] = 0.0  # exclude self-similarity

        max_sim = float(np.max(row_sim))
        max_idx = int(np.argmax(row_sim))

        if max_sim >= threshold:
            score = round((1 - max_sim) * 100, 2)
            similar_id = str(df.iloc[max_idx].get("id", max_idx))
            flagged_ids.append(similar_id)
            flagged_count += 1
            log.debug(
                "Record %s flagged as duplicate of %s (sim=%.3f)",
                df.iloc[i].get("id"), similar_id, max_sim
            )
        else:
            score = 100.0

        scores.append(score)

    return scores, flagged_ids, flagged_count


# ── PostgreSQL ────────────────────────────────────────────────

def update_similarity_scores(engine, record_ids: list[str], scores: list[float], run_date: str):
    """
    Update similarity_score in evaluation_results.
    Records must already exist from Layer 1 (evaluate_heuristics.py).
    """
    with engine.begin() as conn:
        for record_id, score in zip(record_ids, scores):
            conn.execute(
                text("""
                    UPDATE evaluation_results
                    SET similarity_score = :score
                    WHERE record_id = :rid AND batch_date = :d
                """),
                {"score": score, "rid": record_id, "d": run_date}
            )
    log.info("Updated similarity scores for %d records", len(record_ids))


# ── Entry point ───────────────────────────────────────────────

def run(run_date: str = None) -> dict:
    """Entry point — called by Airflow evaluation_dag or directly."""
    if not run_date:
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    df = load_silver(run_date)

    # Cap at MAX_RECORDS to avoid OOM on large batches
    if len(df) > MAX_RECORDS:
        log.warning("Truncating to %d records for similarity (was %d)", MAX_RECORDS, len(df))
        df = df.head(MAX_RECORDS)

    # Build text representations
    texts = build_texts(df)

    # Generate embeddings
    embeddings = generate_embeddings(texts)

    # Compute similarity matrix
    log.info("Computing %dx%d similarity matrix...", len(df), len(df))
    sim_matrix = cosine_similarity_matrix(embeddings)

    # Score each record
    scores, flagged_ids, flagged_count = compute_similarity_scores(
        df, sim_matrix, SIMILARITY_THRESHOLD
    )

    # Update PostgreSQL
    engine = create_engine(DB_URL, pool_pre_ping=True)
    record_ids = [str(row.get("id", i)) for i, (_, row) in enumerate(df.iterrows())]
    update_similarity_scores(engine, record_ids, scores, run_date)

    avg_score = sum(scores) / len(scores) if scores else 0

    summary = {
        "evaluated":   len(df),
        "flagged":     flagged_count,
        "avg_score":   round(avg_score, 2),
        "threshold":   SIMILARITY_THRESHOLD,
        "date":        run_date,
    }
    log.info("Similarity evaluation complete: %s", summary)
    return summary


if __name__ == "__main__":
    print(run())