"""
Ingestion DAG — runs daily at 08:00 UTC.
Calls ingest_api.py (Adzuna) and ingest_scraper.py (BeautifulSoup) in sequence.
Both upload raw JSON to MinIO Bronze bucket.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "matheus",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="ingestion_dag",
    description="Ingest jobs from Adzuna API and scraper → MinIO Bronze",
    schedule_interval="0 8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "bronze", "project4"],
) as dag:

    def run_api():
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from ingestion.ingest_api import run
        result = run()
        print(f"API ingestion result: {result}")

    def run_scraper():
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from ingestion.ingest_scraper import run
        result = run()
        print(f"Scraper ingestion result: {result}")

    task_api = PythonOperator(
        task_id="ingest_adzuna_api",
        python_callable=run_api,
        execution_timeout=timedelta(minutes=15),
    )

    task_scraper = PythonOperator(
        task_id="ingest_remoteok_scraper",
        python_callable=run_scraper,
        execution_timeout=timedelta(minutes=10),
    )

    task_api >> task_scraper