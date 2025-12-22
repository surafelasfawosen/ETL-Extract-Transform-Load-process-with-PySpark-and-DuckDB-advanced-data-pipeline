# advanced_fraud_pipeline.py
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import timedelta
import duckdb
import os

@task(
    retries=3,                  # Auto-retry on failure
    retry_delay_seconds=10,     # Exponential backoff
    timeout_seconds=600,        # Increased for large files
    cache_key_fn=lambda _, params: params.get("file_path"),  # Cache by file path
    cache_expiration=timedelta(days=1),                     # Daily refresh
)
def load_data(source: str, file_path: str):
    logger = get_run_logger()
    spark = SparkSession.builder.appName("FraudLoad").getOrCreate()
    try:
        if source == "paysim":
            df = spark.read.parquet(file_path)
        elif source == "aml":
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        elif source == "currency":
            raw = spark.read.option("wholetext", "true").text(file_path)
            df = raw.select(explode(from_json(col("value"), "MAP<STRING,STRING>")).alias("currency_code", "currency_name"))
        logger.info(f"Loaded {source} data: {df.count()} rows")
        return df
    except Exception as e:
        logger.error(f"Load failed for {source}: {str(e)}")
        raise
    finally:
        spark.stop()

@task(retries=2)
def enrich_and_flag_aml(aml_df, currency_df):
    logger = get_run_logger()
    try:
        enriched = aml_df.join(currency_df, aml_df["Payment Currency"] == currency_df["currency_name"], "left") \
            .withColumn("timestamp", to_timestamp("Timestamp", "yyyy/MM/dd HH:mm")) \
            .drop("Timestamp") \
            .withColumn("hour", hour("timestamp")) \
            .withColumn("is_night_transaction", when((col("hour") < 6) | (col("hour") > 21), 1).otherwise(0)) \
            .withColumn("high_amount_flag", when(col("Amount Paid") > 1_000_000, 1).otherwise(0)) \
            .withColumn("aml_alert_flag", when(col("high_amount_flag") == 1, 1).otherwise(0))
        logger.info("AML enrichment and flagging complete")
        return enriched
    except Exception as e:
        logger.error(f"Enrichment failed: {str(e)}")
        raise

@flow(name="Risk Flagging Subflow", task_runner=ConcurrentTaskRunner())
def risk_flagging_subflow(paysim_df, enriched_aml):
    paysim = paysim_df  # Capture for inner task (removes any linter warnings)
    
    @task
    def cross_ref(paysim, aml):
        fraud_threshold = paysim.filter("isFraud == 1").approxQuantile("amount", [0.95], 0.01)[0]
        return aml.withColumn("paysim_high_risk", when(col("Amount Paid") > fraud_threshold, 1).otherwise(0))
    
    return cross_ref(paysim, enriched_aml)

@task(retries=1, timeout_seconds=600)
def load_to_duckdb(final_df, db_path: str = "C:/Users/hp/Desktop/ess_faiss_index/analytics.duckdb"):
    logger = get_run_logger()
    temp_dir = os.path.join(os.path.dirname(db_path), "temp_final_parquet")
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        logger.info("Writing final enriched data to temporary Parquet...")
        final_df.write.mode("overwrite").parquet(temp_dir)
        
        con = duckdb.connect(db_path)
        con.execute(f"""
            CREATE OR REPLACE TABLE fraud_transactions AS 
            SELECT * FROM read_parquet('{temp_dir}/*.parquet')
        """)
        con.execute("""
            CREATE OR REPLACE VIEW high_risk_alerts AS
            SELECT * FROM fraud_transactions 
            WHERE aml_alert_flag = 1 OR paysim_high_risk = 1
            ORDER BY "Amount Paid" DESC
        """)
        alert_count = con.execute("SELECT COUNT(*) FROM high_risk_alerts").fetchone()[0]
        total_count = con.execute("SELECT COUNT(*) FROM fraud_transactions").fetchone()[0]
        logger.info(f"✅ DuckDB load complete: {total_count:,} total transactions")
        logger.info(f"🚨 {alert_count:,} high-risk alerts flagged")
    except Exception as e:
        logger.error(f"❌ DuckDB load failed: {str(e)}")
        raise
    finally:
        con.close()

@flow(
    name="Advanced Fraud Detection Pipeline",
    description="Orchestrates fraud ETL with advanced resilience and monitoring",
    retries=2,
    task_runner=ConcurrentTaskRunner(),
)
def advanced_fraud_flow(base_path: str = "C:/Users/hp/Desktop/ess_faiss_index"):
    paysim = load_data("paysim", f"{base_path}/fraud_detection.parquet")
    aml = load_data("aml", f"{base_path}/HI-Small_Trans.csv")
    currency = load_data("currency", f"{base_path}/currency.json")
    
    enriched_aml = enrich_and_flag_aml(aml, currency)
    final_df = risk_flagging_subflow(paysim, enriched_aml)
    
    load_to_duckdb(final_df)
    
    logger = get_run_logger()
    logger.info("Advanced fraud detection pipeline completed successfully!")

# Daily automatic execution – modern Prefect way (2025 best practice)
if __name__ == "__main__":
    advanced_fraud_flow.serve(
        name="daily-fraud-detection-2025",
        cron="0 2 * * *",          # Every day at 2:00 AM
        tags=["fraud-detection", "production"],
        description="Runs money laundering detection pipeline daily at 2 AM"
    )