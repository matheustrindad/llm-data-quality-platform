# AI Data Quality & Enrichment Platform

![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?logo=apacheairflow)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark)
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

> **Differentiator:** A custom LLM-as-a-Judge evaluation engine that assesses data fitness for AI training contexts. In production tests, **34% of ingested records were flagged as semantic duplicates** — invisible to exact-match deduplication alone.

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
         └── POST /data/evaluate
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
- **Completeness:** weighted field presence (title=30%, description=25%, company=20%, location=15%, country=10%)
- **Consistency:** format validation (salary ranges, URL format, title length, country codes)
- **Uniqueness:** duplicate ID detection within the batch

**Result on production data:** avg score 94.95/100 — Adzuna API data has strong structural quality.

### Layer 2 — Semantic Similarity
Uses `paraphrase-MiniLM-L6-v2` (384-dimension embeddings) to compute pairwise cosine similarity across all records. Records above 0.92 threshold are flagged as semantic duplicates.

**Result on production data:** 142 of 416 records (34%) flagged — the same job posting republished with slightly different titles across countries.

### Layer 3 — LLM-as-a-Judge
Sends a structured sample to `llama3.2:3b` (local, via Ollama) with an explicit evaluation prompt. The model returns `{score, issues, suggestions}` as structured JSON. Falls back to layers 1+2 if the LLM is unavailable.

**Result on production data:** avg score 76.0/100 on sampled records.

**Final composite score** = heuristic × 0.40 + similarity × 0.25 + LLM × 0.35

Records scoring below 60 are routed to the Feedback Loop for reprocessing.

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | Python, Requests, BeautifulSoup | REST APIs + web scraping |
| Storage | MinIO (S3-compatible) | Bronze / Silver / Gold / Quarantine / Reprocessing buckets |
| Processing | PySpark 3.5.1 | Batch cleaning, validation, aggregation |
| Orchestration | Apache Airflow 2.8 | 4 DAGs with dependency management |
| Data Warehouse | PostgreSQL 15 | Data Mart for analytical queries |
| Evaluation | sentence-transformers + Ollama (llama3.2:3b) | 3-layer quality scoring engine |
| API | NestJS + TypeORM + JWT + Swagger | REST API with auto-generated docs |
| Observability | Prometheus + Grafana | Pipeline metrics and dashboards |
| CI/CD | GitLab CI | Lint, test, build pipeline |
| Infrastructure | Docker Compose | Single-command full stack (7 services) |

---

## Airflow DAGs

| DAG | Schedule | Description |
|---|---|---|
| `ingestion_dag` | 08:00 UTC daily | Scraper + API → MinIO Bronze |
| `processing_dag` | 09:00 UTC daily | Bronze → Silver → Gold (PySpark) |
| `evaluation_dag` | 10:00 UTC daily | 3-layer quality scoring → PostgreSQL |
| `feedback_dag` | Weekly (Sunday) | Re-routes low-score records to reprocessing/ |

---

## API Endpoints

```bash
# Authentication
POST /auth/login
# Body: {"username": "admin", "password": "admin123"}
# Returns: {"access_token": "eyJ..."}

# Job market data (requires JWT)
GET /data?source=adzuna_api&country=US&limit=50

# Quality metrics (requires JWT)
GET /metrics?period=7

# Top skills by country (requires JWT)
GET /metrics/skills?country=US&limit=10

# On-demand evaluation (requires JWT)
POST /data/evaluate
# Body: {"title": "Data Engineer", "description": "PySpark required"}
# Returns: {"score": 85, "issues": ["missing_location"], "passed": true}
```

Interactive API docs (Swagger UI): `http://localhost:3000/docs`

---

## Observability

Prometheus scrapes metrics from the pipeline. Grafana provisioning files are in `infra/grafana/provisioning/` and load automatically at startup.

| Metric | Type | Description |
|---|---|---|
| `pipeline_records_processed_total` | Counter | Total records processed per DAG run |
| `pipeline_processing_duration_seconds` | Histogram | Processing time per stage |
| `pipeline_failed_records_total` | Counter | Records sent to quarantine |
| `evaluation_llm_score_avg` | Gauge | Rolling average LLM evaluation score |
| `feedback_records_reprocessed_total` | Counter | Records re-routed by feedback loop |

---

## How to Run

**Prerequisites:** Docker Desktop, Git, Python 3.10+, Node.js 24+, Ollama

```bash
# 1. Clone the repository
git clone https://github.com/matheustrindad/llm-data-quality-platform.git
cd llm-data-quality-platform

# 2. Set up environment variables
cp .env.example .env
# Edit .env — add ADZUNA keys and generate security keys

# 3. Generate Airflow security keys
python -c "from cryptography.fernet import Fernet; print('FERNET_KEY=' + Fernet.generate_key().decode())"
python -c "import secrets; print('SECRET_KEY=' + secrets.token_hex(32))"

# 4. Pull LLM model (required for Layer 3 evaluation)
ollama pull llama3.2:3b

# 5. Start the full stack
docker-compose up -d

# 6. Start the NestJS API
cd src/api && npm install && npm run start:dev

# 7. Access the services:
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
│   │   ├── process_gold.py        # PySpark: Silver → Gold
│   │   └── load_datamart.py       # Gold → PostgreSQL Data Mart
│   ├── evaluation/
│   │   ├── evaluate_heuristics.py # Layer 1: rule-based scoring
│   │   ├── evaluate_similarity.py # Layer 2: semantic dedup (embeddings)
│   │   ├── evaluate_llm.py        # Layer 3: LLM-as-a-Judge (Ollama)
│   │   └── feedback_loop.py       # Re-routes low-score records
│   └── api/                       # NestJS application
│       └── src/
│           ├── auth/              # JWT authentication
│           ├── data/              # /data endpoints
│           └── metrics/           # /metrics endpoints
├── infra/
│   ├── prometheus/prometheus.yml
│   └── grafana/provisioning/
├── Dockerfile                     # Airflow + Java 17 + PySpark image
├── docker-compose.yml             # 7 services: Airflow, MinIO, PostgreSQL,
│                                  # Prometheus, Grafana
├── requirements.txt
└── .env.example
```

---

## Key Engineering Decisions

**Why MinIO instead of local filesystem?**
MinIO exposes the same S3 API as AWS S3 — the same `boto3` code runs on both. Change the endpoint URL in `.env` and the platform runs on real AWS without code changes.

**Why three evaluation layers?**
Each layer catches different failure modes. Heuristics catch structural issues (missing fields, format errors). Semantic similarity catches content duplicates that exact-match misses — proven by the 34% duplicate rate found in production data. The LLM catches semantic issues that rules can't express.

**Why NestJS instead of FastAPI?**
Project 3 already demonstrates FastAPI. NestJS adds TypeScript, dependency injection, and a module system — patterns from enterprise backends. JWT and Swagger are first-class citizens with minimal boilerplate.

**Why a Feedback Loop?**
Data quality is a cycle, not a checkpoint. Records that fail evaluation are flagged and stored in `reprocessing/` with full failure metadata — never deleted. This preserves the audit trail and enables re-ingestion when the source is corrected.

---

## Roadmap

- [ ] **Grafana Dashboard** — pipeline observability panels with live Prometheus metrics
  (provisioning files ready in `infra/grafana/provisioning/`)
- [ ] **GitLab CI/CD** — automated lint (flake8 + eslint), test (pytest + jest) and build pipeline
  (`.gitlab-ci.yml` structure planned)
- [ ] **Data freshness filter** — ingest only jobs posted in the last 21 days (Bronze layer)
- [ ] **NestJS Docker container** — add NestJS to `docker-compose.yml` for single-command startup