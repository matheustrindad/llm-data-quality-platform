import sys
import os

sys.path.insert(0, "src")
os.environ["MINIO_ENDPOINT"]     = "localhost:9000"
os.environ["DATABASE_URL"]       = "postgresql://airflow:airflow@localhost:5432/airflow"
os.environ["DATABASE_URL_LOCAL"] = "postgresql://airflow:airflow@localhost:5432/airflow"

from evaluation.feedback_loop import run

# Usa threshold alto (80) para garantir que encontramos registros
# mesmo com os dados de alta qualidade da Adzuna
result = run(run_date="2026-06-09", score_threshold=85)
print(result)