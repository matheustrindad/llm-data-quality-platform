import sys
import os

sys.path.insert(0, "src")
os.environ["MINIO_ENDPOINT"]    = "localhost:9000"
os.environ["DATABASE_URL"]      = "postgresql://airflow:airflow@localhost:5432/airflow"
os.environ["DATABASE_URL_LOCAL"] = "postgresql://airflow:airflow@localhost:5432/airflow"
os.environ["OLLAMA_URL"]        = "http://localhost:11434"
os.environ["OLLAMA_MODEL"]      = "llama3.2:3b"

# Avalia apenas 5 registros para teste rápido
os.environ["LLM_SAMPLE_SIZE"] = "5"

from evaluation.evaluate_llm import run

result = run(run_date="2026-06-09")
print(result)