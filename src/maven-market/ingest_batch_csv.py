import os
import sys
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

# Add repository root to Python path to locate utils and config.yml dynamically
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "../../"))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from utils.custom_logger import CustomLogger

# Initialize Spark & Load Config
spark = SparkSession.builder.getOrCreate()
config_path = os.path.join(REPO_ROOT, "config.yml")

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

logger = CustomLogger(spark, "batch_csv_ingestion")
logger.log_start()

try:
    # Auto Loader for Transactions CSV
    raw_tx_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", "dbfs:/checkpoints/schema_transactions")
        .load(config["sources"]["adls"]["csv_transactions"])
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name()))

    # Write stream using availableNow trigger for reliable batch processing
    query = (raw_tx_df.writeStream
        .format("delta")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", "dbfs:/checkpoints/write_transactions")
        .toTable(config["tables"]["bronze_transactions"]))
    
    query.awaitTermination()
    
    # Log total count processed
    rec_count = spark.table(config["tables"]["bronze_transactions"]).count()
    logger.log_success(records_processed=rec_count)

except Exception as e:
    logger.log_failure(e)
    raise e