"""
Feedback DAG — runs weekly on Sunday at 06:00 UTC.
Queries PostgreSQL for records with score < threshold (default: 60).
Re-routes low-quality records back to MinIO reprocessing/ bucket.
Generates a report saved to PostgreSQL: table feedback_history.

The Feedback Loop closes the quality cycle:
  Evaluation → score < 60 → Feedback DAG → reprocessing/ → re-ingestion
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "matheus",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="feedback_dag",
    description="Weekly feedback loop — re-routes low-score records to reprocessing/",
    schedule_interval="0 6 * * 0",   # Every Sunday at 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["feedback", "quality", "loop", "project4"],
) as dag:

    def run_feedback(**context):
        """
        Feedback Loop logic:

        1. Query PostgreSQL evaluation_results for records with
           composite_score < SCORE_THRESHOLD (default 60, set in .env).

        2. For each low-score record:
           - Fetch the original raw JSON from MinIO Bronze
           - Copy it to MinIO reprocessing/ bucket with metadata:
             {original_key, rejection_reasons, scores, feedback_run_date}

        3. Save a feedback report to PostgreSQL feedback_history:
           - total_evaluated: how many records were checked
           - total_reprocessed: how many were below threshold
           - top_rejection_reasons: most common issues found
           - avg_score_before: average score of reprocessed records
           - feedback_run_date: timestamp of this run

        4. Trigger the ingestion_dag to re-ingest reprocessing/ records
           (uses TriggerDagRunOperator pattern — commented out for safety,
            enable when pipeline is stable).

        Why we never delete:
          Records are copied to reprocessing/, never deleted from Bronze.
          This preserves the audit trail and allows debugging why records
          failed evaluation. The reprocessing/ bucket acts as a queue
          for the next ingestion cycle.
        """
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from evaluation.feedback_loop import run

        # Score threshold — records below this are re-routed
        # Configurable via .env: FEEDBACK_SCORE_THRESHOLD=60
        result = run(score_threshold=60)

        print("=" * 50)
        print("FEEDBACK LOOP REPORT")
        print(f"  Total evaluated:    {result.get('total_evaluated', 0)}")
        print(f"  Re-routed:          {result.get('total_reprocessed', 0)}")
        print(f"  Avg score (re-routed): {result.get('avg_score_before', 0):.2f}")
        print(f"  Top reason:         {result.get('top_reason', 'N/A')}")
        print("=" * 50)

        # Fail the DAG if re-processing rate is abnormally high (> 50%)
        total = result.get("total_evaluated", 1)
        reprocessed = result.get("total_reprocessed", 0)
        if total > 0 and (reprocessed / total) > 0.5:
            raise ValueError(
                f"Feedback rate {reprocessed}/{total} ({reprocessed/total:.0%}) "
                f"exceeds 50% — data quality critically degraded, investigate source."
            )

        return result

    PythonOperator(
        task_id="run_feedback_loop",
        python_callable=run_feedback,
        execution_timeout=timedelta(hours=1),
    )