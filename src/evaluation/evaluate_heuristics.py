"""
Evaluation DAG — runs daily at 10:00 UTC (1h after processing).
Applies 3-layer quality scoring to Silver data:
  Layer 1: Heuristics (completeness, consistency, uniqueness)
  Layer 2: Semantic similarity (sentence-transformers)
  Layer 3: LLM-as-a-Judge (Ollama or OpenAI)
Final scores saved to PostgreSQL: table evaluation_results.
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
    dag_id="evaluation_dag",
    description="3-layer data quality evaluation → PostgreSQL evaluation_results",
    schedule_interval="0 10 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["evaluation", "quality", "llm", "project4"],
) as dag:

    def run_heuristics(**context):
        """
        Layer 1 — deterministic rule-based scoring.
        Scores each record 0-100 across three dimensions:
          - Completeness: % of required fields populated
          - Consistency:  format validation (dates, URLs, numeric ranges)
          - Uniqueness:   duplicate rate within the batch
        Pushes results to XCom so Layer 2 can read them.
        """
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from evaluation.evaluate_heuristics import run
        result = run()
        print(f"Heuristics result: {result}")
        # Push record count to XCom for downstream tasks
        context["ti"].xcom_push(key="heuristics_scored", value=result.get("scored", 0))
        return result

    def run_similarity(**context):
        """
        Layer 2 — semantic deduplication via sentence-transformers.
        Generates embeddings for each record's title+description and
        computes pairwise cosine similarity. Records above the threshold
        (default 0.92, configurable via .env) are flagged as semantic duplicates.
        This catches content duplicates that exact-match deduplication misses.
        """
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from evaluation.evaluate_similarity import run
        result = run()
        print(f"Similarity result: {result}")
        context["ti"].xcom_push(key="similarity_flagged", value=result.get("flagged", 0))
        return result

    def run_llm_judge(**context):
        """
        Layer 3 — LLM-as-a-Judge via Ollama (local) or OpenAI (fallback).
        Sends a structured sample of records to the LLM with an explicit
        evaluation prompt. The model returns JSON with:
          {score: 0-100, issues: [...], suggestions: [...]}
        Falls back to layers 1+2 average if LLM is unavailable.
        Final composite score = weighted average of all three layers.
        Saves results to PostgreSQL: table evaluation_results.
        """
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from evaluation.evaluate_llm import run
        result = run()
        print(f"LLM Judge result: {result}")
        context["ti"].xcom_push(key="llm_avg_score", value=result.get("avg_score", 0))
        return result

    def log_evaluation_summary(**context):
        """
        Reads XCom values from all three layers and logs a unified summary.
        This is what appears in the Airflow task log and Grafana dashboard.
        """
        ti = context["ti"]
        heuristics_scored = ti.xcom_pull(task_ids="layer1_heuristics", key="heuristics_scored")
        similarity_flagged = ti.xcom_pull(task_ids="layer2_similarity", key="similarity_flagged")
        llm_avg_score      = ti.xcom_pull(task_ids="layer3_llm_judge",  key="llm_avg_score")

        print("=" * 50)
        print("EVALUATION SUMMARY")
        print(f"  Layer 1 — Records scored:        {heuristics_scored}")
        print(f"  Layer 2 — Semantic duplicates:   {similarity_flagged}")
        print(f"  Layer 3 — LLM avg quality score: {llm_avg_score:.2f}")
        print("=" * 50)

    task_heuristics = PythonOperator(
        task_id="layer1_heuristics",
        python_callable=run_heuristics,
        execution_timeout=timedelta(minutes=15),
    )

    task_similarity = PythonOperator(
        task_id="layer2_similarity",
        python_callable=run_similarity,
        execution_timeout=timedelta(minutes=20),
    )

    task_llm = PythonOperator(
        task_id="layer3_llm_judge",
        python_callable=run_llm_judge,
        execution_timeout=timedelta(minutes=30),
    )

    task_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_evaluation_summary,
    )

    # Layer 1 and 2 run in parallel — both feed into Layer 3
    [task_heuristics, task_similarity] >> task_llm >> task_summary