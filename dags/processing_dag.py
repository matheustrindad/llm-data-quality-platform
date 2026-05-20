"""
Processing DAG — runs daily at 09:00 UTC (1h after ingestion).
Bronze (raw JSON) → Silver (clean Parquet) → Gold (aggregations)
Both layers stored in MinIO.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "matheus",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="processing_dag",
    description="PySpark: Bronze → Silver → Gold in MinIO",
    schedule_interval="0 9 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["processing", "silver", "gold", "project4"],
) as dag:

    def run_silver():
        import sys, os
        sys.path.insert(0, "/opt/airflow/src")
        os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
        from processing.process_silver import run
        result = run()
        print(f"Silver result: {result}")
        if result.get("valid", 0) == 0:
            raise ValueError("Zero valid records in Silver — check Bronze data")

    def run_gold():
        import sys, os
        sys.path.insert(0, "/opt/airflow/src")
        os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
        from processing.process_gold import run
        result = run()
        print(f"Gold result: {result}")

    task_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_silver,
        execution_timeout=timedelta(minutes=30),
    )

    task_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_gold,
        execution_timeout=timedelta(minutes=20),
    )

    task_silver >> task_gold