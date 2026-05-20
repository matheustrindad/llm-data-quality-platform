"""
Gold layer processing — reads Silver Parquet from MinIO and generates
3 analytical aggregations saved back to MinIO Gold bucket.

Silver  →  process_gold.py  →  Gold (3 aggregation tables)
                             →  PostgreSQL Data Mart (load_datamart.py)

Gold aggregations:
  1. metrics_by_source    — volume and salary stats per source/country/date
  2. top_skills           — most demanded skills extracted from job titles
  3. quality_summary      — completeness metrics per batch (feeds Evaluation Engine)
"""

import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("process_gold")

MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SILVER_BUCKET     = os.getenv("MINIO_BUCKET_SILVER", "silver")
GOLD_BUCKET       = os.getenv("MINIO_BUCKET_GOLD", "gold")

# Skills to detect in job titles — used for top_skills aggregation
SKILL_KEYWORDS = [
    "python", "sql", "spark", "airflow", "kafka", "docker", "kubernetes",
    "aws", "azure", "gcp", "dbt", "snowflake", "databricks", "pandas",
    "scala", "java", "terraform", "postgresql", "mongodb", "redis",
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("LLM-DataPlatform-Gold")
        .config("spark.hadoop.fs.s3a.endpoint",          f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def agg_metrics_by_source(df: DataFrame) -> DataFrame:
    """
    Aggregation 1 — metrics_by_source.
    Answers: "How many jobs per source, per country, per day?
              What's the average salary? How many are remote?"

    This is the primary table consumed by the NestJS API /metrics endpoint.
    """
    return (
        df.groupBy("_source", "country", "posted_date")
        .agg(
            F.count("*").alias("job_count"),
            F.countDistinct("company").alias("company_count"),
            F.round(F.avg("salary_min"), 2).alias("avg_salary_min"),
            F.round(F.avg("salary_max"), 2).alias("avg_salary_max"),
            F.round(F.stddev("salary_min"), 2).alias("stddev_salary"),
            F.sum(F.col("is_remote").cast("int")).alias("remote_count"),
            # Remote rate — useful for trend analysis
            F.round(
                F.sum(F.col("is_remote").cast("int")) / F.count("*") * 100, 1
            ).alias("remote_pct"),
        )
        .withColumnRenamed("_source", "source")
        .orderBy("posted_date", "country", "source")
    )


def agg_top_skills(df: DataFrame) -> DataFrame:
    """
    Aggregation 2 — top_skills.
    Extracts skill mentions from job titles and counts frequency
    per country and source.

    Why title only (not description)?
    Descriptions are long and noisy — titles are the most signal-dense
    field for skill detection without requiring NLP.

    This feeds the Evaluation Engine's semantic layer as a baseline
    for what "good" job data looks like.
    """
    skill_dfs = []
    for skill in SKILL_KEYWORDS:
        skill_df = (
            df.filter(F.lower(F.col("title")).rlike(f"\\b{skill}\\b"))
            .groupBy("country", "_source")
            .agg(F.count("*").alias("mention_count"))
            .withColumn("skill", F.lit(skill))
            .withColumnRenamed("_source", "source")
        )
        skill_dfs.append(skill_df)

    from functools import reduce
    combined = reduce(DataFrame.unionAll, skill_dfs)
    return (
        combined
        .groupBy("country", "source", "skill")
        .agg(F.sum("mention_count").alias("mention_count"))
        .orderBy("country", F.col("mention_count").desc())
    )


def agg_quality_summary(df: DataFrame) -> DataFrame:
    """
    Aggregation 3 — quality_summary.
    Computes completeness metrics per batch — what % of records
    have each field populated.

    Why this matters for the Evaluation Engine:
    This table is the baseline the heuristic scorer uses to compare
    individual records against the batch average. A record with 60%
    field completeness in a batch where the average is 90% is a
    clear quality outlier.
    """
    total = df.count()
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    return df.agg(
        F.lit(run_date).alias("batch_date"),
        F.lit(total).alias("total_records"),
        # Completeness per field — % of non-null values
        F.round(F.count("title")       / total * 100, 2).alias("pct_has_title"),
        F.round(F.count("company")     / total * 100, 2).alias("pct_has_company"),
        F.round(F.count("description") / total * 100, 2).alias("pct_has_description"),
        F.round(F.count("salary_min")  / total * 100, 2).alias("pct_has_salary"),
        F.round(F.count("location")    / total * 100, 2).alias("pct_has_location"),
        # Seniority distribution
        F.round(F.sum((F.col("seniority") == "senior").cast("int")) / total * 100, 2).alias("pct_senior"),
        F.round(F.sum((F.col("seniority") == "mid").cast("int"))    / total * 100, 2).alias("pct_mid"),
        F.round(F.sum((F.col("seniority") == "junior").cast("int")) / total * 100, 2).alias("pct_junior"),
        # Remote rate
        F.round(F.sum(F.col("is_remote").cast("int")) / total * 100, 2).alias("pct_remote"),
    )


def run(run_date: str = None) -> dict:
    """Entry point — called by Airflow processing_dag or directly."""
    if not run_date:
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    silver_path = f"s3a://{SILVER_BUCKET}/jobs/{run_date}/"
    log.info("Reading Silver: %s", silver_path)

    try:
        df = spark.read.parquet(silver_path)
        total = df.count()
        log.info("Silver records loaded: %d", total)
    except Exception as e:
        log.error("Failed to read Silver: %s", e)
        spark.stop()
        return {"error": str(e)}

    results = {}

    # ── Aggregation 1: metrics by source ────────────────────
    metrics_df = agg_metrics_by_source(df)
    metrics_path = f"s3a://{GOLD_BUCKET}/metrics_by_source/{run_date}/"
    metrics_df.write.mode("overwrite").parquet(metrics_path)
    results["metrics_rows"] = metrics_df.count()
    log.info("metrics_by_source → %d rows", results["metrics_rows"])

    # ── Aggregation 2: top skills ────────────────────────────
    skills_df = agg_top_skills(df)
    skills_path = f"s3a://{GOLD_BUCKET}/top_skills/{run_date}/"
    skills_df.write.mode("overwrite").parquet(skills_path)
    results["skills_rows"] = skills_df.count()
    log.info("top_skills → %d rows", results["skills_rows"])

    # ── Aggregation 3: quality summary ───────────────────────
    quality_df = agg_quality_summary(df)
    quality_path = f"s3a://{GOLD_BUCKET}/quality_summary/{run_date}/"
    quality_df.write.mode("overwrite").parquet(quality_path)
    log.info("quality_summary written")
    results["quality_rows"] = 1

    spark.stop()
    log.info("Gold processing complete: %s", results)
    return results


if __name__ == "__main__":
    print(run())