import os
import sys
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

# Safely resolve script directory in both interactive and job contexts
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    SCRIPT_DIR = os.getcwd()

# Traversal search upward to locate repository root containing config.yml
REPO_ROOT = SCRIPT_DIR
while REPO_ROOT != os.path.dirname(REPO_ROOT):
    if os.path.exists(os.path.join(REPO_ROOT, "config.yml")):
        break
    REPO_ROOT = os.path.dirname(REPO_ROOT)

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
    # Build ADLS Gen2 Checkpoint Paths dynamically
    base_adls = config["sources"]["adls"]["base_path"].rstrip("/")
    schema_path = f"{base_adls}/checkpoints/schema_transactions"
    checkpoint_path = f"{base_adls}/checkpoints/write_transactions"

    # Auto Loader for Transactions CSV
    raw_tx_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", schema_path)
        .load(config["sources"]["adls"]["csv_transactions"])
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path")))

    # Write stream using availableNow trigger for reliable batch processing
    query = (raw_tx_df.writeStream
        .format("delta")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_path)
        .toTable(config["tables"]["bronze_transactions"]))
    
    query.awaitTermination()
    
    # Log total count processed
    rec_count = spark.table(config["tables"]["bronze_transactions"]).count()
    logger.log_success(records_processed=rec_count)

except Exception as e:
    logger.log_failure(e)
    raise e


