# AI Data Quality & Enrichment Platform

![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?logo=apacheairflow)
![PySpark](https://img.shields.io/badge/PySpark-3.x-E25A1C?logo=apachespark)
![MinIO](https://img.shields.io/badge/MinIO-S3--Compatible-C72E49?logo=minio)
![NestJS](https://img.shields.io/badge/NestJS-TypeScript-E0234E?logo=nestjs)
![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C?logo=prometheus)
![Grafana](https://img.shields.io/badge/Grafana-Dashboards-F46800?logo=grafana)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![GitLab CI](https://img.shields.io/badge/GitLab-CI%2FCD-FC6D26?logo=gitlab)

A production-grade AI data platform that ingests, cleans, evaluates and enriches data automatically using LLMs — built to solve the "Garbage In, Garbage Out" problem for AI systems.

---

## The Problem

In the era of LLMs and RAG (Retrieval-Augmented Generation), dirty data is more expensive than ever. Training or fine-tuning models on inconsistent, duplicate, or low-context data burns compute and degrades performance.

This platform doesn't just move data from A to B — it **evaluates** it using three layers of defense before allowing it to reach the Data Mart or Training Pipeline.

> **Differentiator:** A custom LLM-as-a-Judge evaluation engine that assesses data fitness for AI training contexts.

---

## Architecture

```
Scraping (BeautifulSoup) + REST APIs
                ↓
       MinIO — Bronze Layer
       [raw JSON, partitioned by source/date]
                ↓
      PySpark process_silver.py
      ├── Schema enforcement
      ├── HTML cleaning
      ├── Validation & quarantine
      ├── Deduplication (window functions)
      └── Seniority + remote flag extraction
                ↓
       MinIO — Silver Layer (Parquet)
                ↓
      PySpark process_gold.py
      (aggregations: volume, salary, skills)
                ↓
       MinIO — Gold Layer (Parquet)
                ↓
      PostgreSQL Data Mart
                ↓
      ┌─────────────────────────────────┐
      │       Evaluation Engine         │
      ├─────────────────────────────────┤
      │ 1. Heuristics (completeness,    │
      │    consistency, uniqueness)     │
      │ 2. Semantic similarity          │
      │    (sentence-transformers)      │
      │ 3. LLM-as-a-Judge               │
      │    (Ollama local / OpenAI)      │
      └────────────┬────────────────────┘
                   ↓
         score < 60 → Feedback Loop
         (reprocessing/ bucket in MinIO)
                   ↓
         NestJS API (JWT protected)
         ├── POST /auth/login
         ├── GET  /data
         ├── GET  /metrics
         └── POST /evaluate
                   ↓
      Prometheus → Grafana (observability)

      Apache Airflow orchestrates every stage
      GitLab CI/CD automates lint, test, build
```

---

## Data Quality Framework

The evaluation engine applies three independent scoring layers before data reaches production:

### Layer 1 — Heuristics (deterministic)
Rule-based scoring across three dimensions:
- **Completeness:** percentage of required fields populated
- **Consistency:** format validation (dates, URLs, numeric ranges)
- **Uniqueness:** duplicate rate within the batch

### Layer 2 — Semantic Similarity
Uses `sentence-transformers` to generate embeddings and detect records that are semantically identical despite having different field values — catching duplicates that exact-match deduplication misses.

### Layer 3 — LLM-as-a-Judge
Sends a structured sample to a local LLM (Ollama) with an explicit evaluation prompt. The model returns a JSON with `score`, `issues`, and `suggestions`. Falls back to layers 1+2 if the LLM is unavailable.

**Final score** = weighted average of the three layers. Records scoring below 60 are routed to the Feedback Loop for reprocessing.

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | Python, Requests, BeautifulSoup | REST APIs + web scraping |
| Storage | MinIO (S3-compatible) | Bronze / Silver / Gold / Quarantine buckets |
| Processing | PySpark 3.x | Batch cleaning, validation, aggregation |
| Orchestration | Apache Airflow 2.8 | 4 DAGs with TriggerDagRunOperator |
| Data Warehouse | PostgreSQL 15 | Data Mart for analytical queries |
| Evaluation | sentence-transformers + Ollama | 3-layer quality scoring engine |
| API | NestJS + TypeORM + JWT | REST API with Swagger docs |
| Observability | Prometheus + Grafana | Pipeline metrics and dashboards |
| CI/CD | GitLab CI | Lint, test, build pipeline |
| Infrastructure | Docker Compose | Single-command full stack |

---

## Airflow DAGs

| DAG | Schedule | Description |
|---|---|---|
| `ingestion_dag` | 08:00 UTC daily | Scraper + API → MinIO Bronze |
| `processing_dag` | 09:00 UTC daily | Bronze → Silver → Gold |
| `evaluation_dag` | 10:00 UTC daily | 3-layer quality scoring → PostgreSQL |
| `feedback_dag` | Weekly (Sunday) | Re-routes low-score records to reprocessing/ |

---

## API Endpoints

```bash
# Authentication
POST /auth/login
# Returns JWT token for protected endpoints

# Data (requires JWT)
GET /data?source=adzuna&country=us&limit=50

# Quality metrics (requires JWT)
GET /metrics?period=7d&source=adzuna

# On-demand evaluation (requires JWT)
POST /evaluate
Content-Type: application/json
{"title": "Data Engineer", "description": "PySpark experience required..."}
# Returns: {"score": 87, "issues": [], "suggestions": [...]}
```

Interactive API docs (Swagger UI) available at `http://localhost:3000/docs`

---

## Observability

Prometheus scrapes metrics exposed by the pipeline scripts:

| Metric | Type | Description |
|---|---|---|
| `pipeline_records_processed_total` | Counter | Total records processed per DAG run |
| `pipeline_processing_duration_seconds` | Histogram | Processing time per stage |
| `pipeline_failed_records_total` | Counter | Records sent to quarantine |
| `evaluation_llm_score_avg` | Gauge | Rolling average LLM evaluation score |
| `feedback_records_reprocessed_total` | Counter | Records re-routed by feedback loop |

Grafana dashboards are provisioned automatically at startup via `infra/grafana/provisioning/`.

---

## How to Run

**Prerequisites:** Docker Desktop, Git, Python 3.10+

```bash
# 1. Clone the repository
git clone https://github.com/matheustrindad/llm-data-quality-platform.git
cd llm-data-quality-platform

# 2. Set up environment variables
cp .env.example .env
# Edit .env — add ADZUNA keys and generate security keys (see below)

# 3. Generate Airflow security keys
python -c "from cryptography.fernet import Fernet; print('FERNET_KEY=' + Fernet.generate_key().decode())"
python -c "import secrets; print('SECRET_KEY=' + secrets.token_hex(32))"

# 4. Start the full stack
docker-compose up -d

# 5. Access the services:
#    Airflow UI    → http://localhost:8080  (airflow / airflow)
#    MinIO Console → http://localhost:9001  (minioadmin / minioadmin)
#    Grafana       → http://localhost:3001  (admin / admin)
#    Prometheus    → http://localhost:9090
#    NestJS API    → http://localhost:3000/docs
```

---

## Project Structure

```
llm-data-quality-platform/
├── dags/
│   ├── ingestion_dag.py
│   ├── processing_dag.py
│   ├── evaluation_dag.py
│   └── feedback_dag.py
├── src/
│   ├── ingestion/
│   │   ├── ingest_api.py          # Adzuna multi-country ingestion
│   │   └── ingest_scraper.py      # BeautifulSoup scraper
│   ├── processing/
│   │   ├── process_silver.py      # PySpark: Bronze → Silver
│   │   └── process_gold.py        # PySpark: Silver → Gold
│   ├── evaluation/
│   │   ├── evaluate_heuristics.py # Layer 1: rule-based scoring
│   │   ├── evaluate_similarity.py # Layer 2: semantic dedup
│   │   └── evaluate_llm.py        # Layer 3: LLM-as-a-Judge
│   └── api/                       # NestJS application
├── infra/
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│       └── provisioning/
│           └── datasources/
├── Dockerfile                     # Airflow + Java 17 image
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## Key Engineering Decisions

**Why MinIO instead of local filesystem?**
MinIO exposes the same S3 API as AWS S3 — the same `boto3` code runs on both. This makes the platform production-portable: change the endpoint URL in `.env` and it runs on real AWS without code changes.

**Why three evaluation layers?**
Each layer catches different failure modes. Heuristics catch structural issues (missing fields, format errors). Semantic similarity catches content duplicates that exact-match misses. The LLM catches semantic issues that rules can't express — like a job description that's actually a spam post.

**Why NestJS instead of FastAPI?**
The Projeto 3 already demonstrates FastAPI. NestJS adds TypeScript, dependency injection, and a module system — patterns from enterprise backends. JWT authentication and Swagger are first-class citizens in NestJS with minimal boilerplate.

**Why Prometheus + Grafana?**
A pipeline without observability is a black box in production. Exposing metrics from the Python scripts and visualizing them in Grafana demonstrates that you know how to operate pipelines, not just build them.

**Why a Feedback Loop?**
Data quality is not a one-time check — it's a cycle. Records that fail evaluation don't get deleted; they get flagged, stored in a `reprocessing/` bucket, and re-ingested after the source is corrected. This mirrors how production data quality systems work.