"""
Layer 3 — LLM-as-a-Judge Evaluation.

Sends a sample of Silver records to a local LLM (Ollama) with a
structured evaluation prompt. The LLM acts as a data quality judge,
returning a JSON with score, issues, and suggestions for each record.

Why LLM as a judge?
  Heuristics (Layer 1) catch structural problems — missing fields,
  invalid formats. Semantic similarity (Layer 2) catches content
  duplicates. But neither catches semantic quality issues like:
    - A "Data Engineer" job that's actually a sales role
    - A description that's just copy-pasted boilerplate
    - A salary that's suspiciously low for the seniority level
    - A job title that's misleading or vague
  The LLM can reason about these in natural language.

Strategy:
  - Evaluate a SAMPLE of records (default: 50) — LLM is slow on CPU
  - Use structured JSON output for reliable parsing
  - Fallback to Layer 1+2 average if LLM is unavailable
  - Update llm_score and composite_score in evaluation_results

Model: llama3.2:3b (local, free, no API key needed)
Fallback: openai gpt-4o-mini (if OPENAI_API_KEY is set)
"""

import json
import logging
import os
import io
import time
from datetime import datetime, timezone

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("evaluate_llm")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SILVER_BUCKET    = os.getenv("MINIO_BUCKET_SILVER", "silver")

DB_URL = os.getenv(
    "DATABASE_URL",
    os.getenv("DATABASE_URL_LOCAL", "postgresql://airflow:airflow@localhost:5432/airflow")
)

OLLAMA_URL    = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL  = os.getenv("OLLAMA_MODEL", "llama3.2:3b")
OPENAI_KEY    = os.getenv("OPENAI_API_KEY")

# How many records to send to the LLM — CPU inference is slow
# 50 records × ~3s each ≈ 2.5 minutes on CPU
LLM_SAMPLE_SIZE = int(os.getenv("LLM_SAMPLE_SIZE", "50"))

# Composite score weights (Layer 1 + 2 + 3)
COMPOSITE_WEIGHTS = {
    "heuristic":   0.40,
    "similarity":  0.25,
    "llm":         0.35,
}

# ── Prompt engineering ────────────────────────────────────────

SYSTEM_PROMPT = """You are a data quality expert evaluating job postings for a data engineering platform.
Your task is to assess whether a job posting record is high quality and useful for market analysis.

You must respond ONLY with a valid JSON object. No explanation, no markdown, no extra text.

Evaluate based on:
1. Title clarity — is the job title clear and specific?
2. Description quality — is the description informative (not just boilerplate)?
3. Data completeness — are important fields like salary or location present?
4. Relevance — is this actually a data/tech role?

JSON format (respond exactly like this):
{
  "score": <integer 0-100>,
  "issues": [<string>, ...],
  "suggestions": [<string>, ...]
}

score: 0=completely unusable, 60=acceptable, 80=good, 100=perfect
issues: list of specific problems found (empty list if none)
suggestions: list of improvements that could be made (empty list if none)"""


def build_prompt(row: pd.Series) -> str:
    """Build the user prompt for a single job record."""
    title   = str(row.get("title", "N/A"))[:200]
    company = str(row.get("company", "N/A"))[:100]
    country = str(row.get("country", "N/A"))
    desc    = str(row.get("description", "N/A"))[:500]
    sal_min = row.get("salary_min")
    sal_max = row.get("salary_max")
    senior  = str(row.get("seniority", "N/A"))

    salary_str = "Not provided"
    if pd.notna(sal_min) and pd.notna(sal_max):
        salary_str = f"${sal_min:,.0f} - ${sal_max:,.0f}"
    elif pd.notna(sal_min):
        salary_str = f"From ${sal_min:,.0f}"

    return f"""Evaluate this job posting:

Title: {title}
Company: {company}
Country: {country}
Seniority: {senior}
Salary: {salary_str}
Description: {desc}"""


# ── LLM clients ───────────────────────────────────────────────

def call_ollama(prompt: str) -> dict:
    """
    Call local Ollama API.
    Ollama runs at localhost:11434 by default.
    Uses the /api/generate endpoint (not chat) for simpler response parsing.
    """
    payload = {
        "model":  OLLAMA_MODEL,
        "prompt": prompt,
        "system": SYSTEM_PROMPT,
        "stream": False,
        "options": {
            "temperature": 0.1,    # Low temperature = more deterministic JSON
            "num_predict": 200,    # Max tokens — JSON response is short
        }
    }
    resp = requests.post(
        f"{OLLAMA_URL}/api/generate",
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    raw = resp.json().get("response", "")
    return parse_llm_response(raw)


def call_openai(prompt: str) -> dict:
    """
    Fallback to OpenAI API if OPENAI_API_KEY is set.
    Uses gpt-4o-mini — cheap and fast.
    """
    import openai
    client = openai.OpenAI(api_key=OPENAI_KEY)
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        temperature=0.1,
        max_tokens=200,
        response_format={"type": "json_object"},
    )
    raw = response.choices[0].message.content
    return parse_llm_response(raw)


def parse_llm_response(raw: str) -> dict:
    """
    Parse LLM JSON response safely.

    LLMs sometimes wrap JSON in markdown code blocks or add extra text.
    This parser handles the common failure modes:
      - ```json ... ``` wrappers
      - Extra text before/after the JSON
      - Malformed JSON → returns a safe default
    """
    # Strip markdown code blocks if present
    raw = raw.strip()
    if "```" in raw:
        start = raw.find("{")
        end   = raw.rfind("}") + 1
        raw   = raw[start:end] if start != -1 else raw

    try:
        parsed = json.loads(raw)
        return {
            "score":       int(parsed.get("score", 50)),
            "issues":      list(parsed.get("issues", [])),
            "suggestions": list(parsed.get("suggestions", [])),
        }
    except (json.JSONDecodeError, ValueError):
        log.warning("Failed to parse LLM response: %s", raw[:100])
        return {"score": 50, "issues": ["llm_parse_error"], "suggestions": []}


def evaluate_record(row: pd.Series) -> dict:
    """
    Evaluate a single record — tries Ollama first, then OpenAI, then fallback.
    """
    prompt = build_prompt(row)

    try:
        return call_ollama(prompt)
    except Exception as e:
        log.warning("Ollama failed: %s — trying OpenAI", e)

    if OPENAI_KEY:
        try:
            return call_openai(prompt)
        except Exception as e:
            log.warning("OpenAI failed: %s — using fallback score", e)

    # Both LLMs unavailable — return neutral score
    return {"score": 50, "issues": ["llm_unavailable"], "suggestions": []}


# ── Data loading ──────────────────────────────────────────────

def load_silver(run_date: str) -> pd.DataFrame:
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    prefix = f"jobs/{run_date}/"
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
    log.info("Loaded %d records from Silver", len(df))
    return df


# ── PostgreSQL ────────────────────────────────────────────────

def update_llm_scores(engine, results: list[dict], run_date: str):
    """
    Update llm_score, llm_issues, llm_suggestions and recompute
    composite_score using all three layer weights.
    """
    with engine.begin() as conn:
        for r in results:
            # Fetch existing heuristic and similarity scores
            row = conn.execute(
                text("""
                    SELECT heuristic_score, similarity_score
                    FROM evaluation_results
                    WHERE record_id = :rid AND batch_date = :d
                """),
                {"rid": r["record_id"], "d": run_date}
            ).fetchone()

            if not row:
                continue

            h_score = float(row[0] or 50)
            s_score = float(row[1] or 100)
            l_score = float(r["llm_score"])

            # Recompute composite with all three layers
            composite = round(
                h_score * COMPOSITE_WEIGHTS["heuristic"] +
                s_score * COMPOSITE_WEIGHTS["similarity"] +
                l_score * COMPOSITE_WEIGHTS["llm"],
                2
            )

            conn.execute(
                text("""
                    UPDATE evaluation_results
                    SET llm_score        = :llm,
                        composite_score  = :comp,
                        llm_issues       = :issues,
                        llm_suggestions  = :suggestions
                    WHERE record_id = :rid AND batch_date = :d
                """),
                {
                    "llm":         l_score,
                    "comp":        composite,
                    "issues":      r["llm_issues"],
                    "suggestions": r["llm_suggestions"],
                    "rid":         r["record_id"],
                    "d":           run_date,
                }
            )
    log.info("Updated LLM scores for %d records", len(results))


# ── Entry point ───────────────────────────────────────────────

def run(run_date: str = None) -> dict:
    """Entry point — called by Airflow evaluation_dag or directly."""
    if not run_date:
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    df = load_silver(run_date)

    # Sample — LLM evaluation is slow on CPU
    sample = df.sample(
        n=min(LLM_SAMPLE_SIZE, len(df)),
        random_state=42
    )
    log.info("Evaluating %d records with LLM (%s)...", len(sample), OLLAMA_MODEL)

    results = []
    for i, (_, row) in enumerate(sample.iterrows()):
        record_id = str(row.get("id", i))

        start = time.time()
        llm_result = evaluate_record(row)
        elapsed = time.time() - start

        log.info(
            "[%d/%d] record=%s score=%d elapsed=%.1fs",
            i + 1, len(sample), record_id[:20], llm_result["score"], elapsed
        )

        results.append({
            "record_id":       record_id,
            "llm_score":       llm_result["score"],
            "llm_issues":      llm_result["issues"],
            "llm_suggestions": llm_result["suggestions"],
        })

    engine = create_engine(DB_URL, pool_pre_ping=True)
    update_llm_scores(engine, results, run_date)

    avg_score = sum(r["llm_score"] for r in results) / len(results) if results else 0
    below_60  = sum(1 for r in results if r["llm_score"] < 60)

    summary = {
        "evaluated":      len(results),
        "avg_score":      round(avg_score, 2),
        "below_threshold": below_60,
        "model":          OLLAMA_MODEL,
        "date":           run_date,
    }
    log.info("LLM evaluation complete: %s", summary)
    return summary


if __name__ == "__main__":
    print(run())