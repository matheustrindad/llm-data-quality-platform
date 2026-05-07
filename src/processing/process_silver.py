"""
Silver layer processing — reads Bronze JSON from MinIO, validates,
cleans and writes Parquet back to MinIO Silver + Quarantine buckets.

Bronze  →  process_silver.py  →  Silver (clean Parquet)
                               →  Quarantine (rejected records + reason)
"""

import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, FloatType, StringType, StructField, StructType,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("process_silver")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET    = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
SILVER_BUCKET    = os.getenv("MINIO_BUCKET_SILVER", "silver")
QUARANTINE_BUCKET = os.getenv("MINIO_BUCKET_QUARANTINE", "quarantine")

# Schema enforcement — rejects records that don't match at read time
BRONZE_SCHEMA = StructType([
    StructField("id",           StringType(), True),
    StructField("title",        StringType(), True),
    StructField("company",      StringType(), True),
    StructField("description",  StringType(), True),
    StructField("location",     StringType(), True),
    StructField("city",         StringType(), True),
    StructField("salary_min",   FloatType(),  True),
    StructField("salary_max",   FloatType(),  True),
    StructField("redirect_url", StringType(), True),
    StructField("_country",     StringType(), True),
    StructField("_source",      StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


def build_spark() -> SparkSession:
    """SparkSession configured to read/write from MinIO via S3A."""
    return (
        SparkSession.builder
        .appName("LLM-DataPlatform-Silver")
        .config("spark.hadoop.fs.s3a.endpoint",          f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Required JARs — must be present in SPARK_HOME/jars or added via packages
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def clean(df: DataFrame) -> DataFrame:
    """Normalize and enrich columns."""
    return (
        df
        .withColumn("title",       F.trim(F.col("title")))
        .withColumn("company",     F.trim(F.col("company")))
        .withColumn("description", F.regexp_replace(F.col("description"), r"<[^>]*>", ""))
        .withColumn("country",     F.upper(F.trim(F.col("_country"))))
        .withColumn("ingested_at", F.col("_ingested_at").cast("timestamp"))
        .withColumn("processed_at", F.current_timestamp())
        # Seniority extraction from title
        .withColumn("seniority",
            F.when(F.col("title").rlike(r"(?i)(senior|sr\.?|lead|principal)"), "senior")
             .when(F.col("title").rlike(r"(?i)(junior|jr\.?|entry.level|intern)"), "junior")
             .otherwise("mid"))
        # Remote flag
        .withColumn("is_remote",
            F.col("country").eqNullSafe("REMOTE") |
            F.lower(F.coalesce(F.col("location"), F.lit(""))).rlike(r"remote|anywhere"))
        .drop("_country", "_ingested_at")
    )


def validate(df: DataFrame):
    """
    Split into (valid, quarantine) DataFrames.
    Quarantine rules:
      - title is null or empty
      - description is null or empty
      - salary_max > 500,000 (extreme outlier)
    """
    missing_title  = F.col("title").isNull() | (F.length(F.trim(F.col("title"))) == 0)
    missing_desc   = F.col("description").isNull() | (F.length(F.trim(F.col("description"))) == 0)
    extreme_salary = F.col("salary_max") > 500_000

    rejection_reason = (
        F.when(missing_title,  "MISSING_TITLE")
         .when(missing_desc,   "MISSING_DESCRIPTION")
         .when(extreme_salary, "EXTREME_SALARY_OUTLIER")
    )

    is_invalid = missing_title | missing_desc | extreme_salary

    quarantine = (
        df.filter(is_invalid)
          .withColumn("rejection_reason", rejection_reason)
          .withColumn("quarantined_at", F.lit(datetime.now(timezone.utc).isoformat()))
    )
    valid = df.filter(~is_invalid)
    return valid, quarantine


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep latest record per (id, country) based on ingested_at."""
    from pyspark.sql.window import Window
    w = Window.partitionBy("id", "country").orderBy(F.col("ingested_at").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def run(run_date: str = None) -> dict:
    """Entry point — called by Airflow DAG or directly."""
    if not run_date:
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    input_path = f"s3a://{BRONZE_BUCKET}/adzuna/*/{run_date}.json"
    log.info("Reading Bronze: %s", input_path)

    try:
        df_raw = spark.read.schema(BRONZE_SCHEMA).json(input_path)
        total = df_raw.count()
        log.info("Bronze records loaded: %d", total)
    except Exception as e:
        log.error("Failed to read Bronze: %s", e)
        spark.stop()
        return {"error": str(e)}

    df_clean          = clean(df_raw)
    df_valid, df_quar = validate(df_clean)
    df_deduped        = deduplicate(df_valid)

    valid_count = df_deduped.count()
    quar_count  = df_quar.count()

    # Write Silver — partitioned by country for efficient downstream reads
    silver_path = f"s3a://{SILVER_BUCKET}/jobs/{run_date}/"
    df_deduped.write.mode("overwrite").partitionBy("country").parquet(silver_path)
    log.info("Silver written → %s (%d records)", silver_path, valid_count)

    # Write Quarantine
    if quar_count > 0:
        quar_path = f"s3a://{QUARANTINE_BUCKET}/jobs/{run_date}/"
        df_quar.write.mode("overwrite").parquet(quar_path)
        log.info("Quarantine written → %s (%d records)", quar_path, quar_count)

    spark.stop()

    result = {"valid": valid_count, "quarantined": quar_count, "date": run_date}
    log.info("Silver processing complete: %s", result)
    return result


if __name__ == "__main__":
    print(run())