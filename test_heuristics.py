import sys
import os

sys.path.insert(0, "src")
os.environ["MINIO_ENDPOINT"] = "localhost:9000"
os.environ["DATABASE_URL"] = "postgresql://airflow:airflow@localhost:5432/airflow"
os.environ["DATABASE_URL_LOCAL"] = "postgresql://airflow:airflow@localhost:5432/airflow"

# Cria as tabelas do Data Mart antes de avaliar
from processing.load_datamart import ensure_tables
from sqlalchemy import create_engine
engine = create_engine("postgresql://airflow:airflow@localhost:5432/airflow")
ensure_tables(engine)
print("Tables created.")

# Roda a avaliação heurística
from evaluation.evaluate_heuristics import run
result = run(run_date="2026-06-09")
print(result)