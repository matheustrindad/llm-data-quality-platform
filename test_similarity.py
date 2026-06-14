import sys
import os

sys.path.insert(0, "src")
os.environ["MINIO_ENDPOINT"] = "localhost:9000"
os.environ["DATABASE_URL"] = "postgresql://airflow:airflow@localhost:5432/airflow"
os.environ["DATABASE_URL_LOCAL"] = "postgresql://airflow:airflow@localhost:5432/airflow"

from evaluation.evaluate_similarity import run

result = run(run_date="2026-06-09")
print(result)