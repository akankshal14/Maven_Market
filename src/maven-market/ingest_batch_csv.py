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

base_adls = config["sources"]["adls"]["base_path"].rstrip("/")
adls_sources = config["sources"]["adls"]

try:
    # Dynamically process every source key starting with 'csv_'
    for key, source_path in adls_sources.items():
        if not key.startswith("csv_"):
            continue

        entity_name = key.replace("csv_", "")

        # Look up table name in config['tables'] or fall back to default Unity Catalog naming
        target_table = config.get("tables", {}).get(
            f"bronze_{entity_name}", 
            f"maven_market_uc.bronze.{entity_name}"
        )

        # Isolated schema inferencing and streaming checkpoints per dataset
        schema_path = f"{base_adls}/checkpoints/schema_{entity_name}"
        checkpoint_path = f"{base_adls}/checkpoints/write_{entity_name}"

        # Ingest CSV stream using Auto Loader
        raw_df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaLocation", schema_path)
            .load(source_path)
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
        )

        # Write batch and block until finished for this table
        query = (
            raw_df.writeStream.format("delta")
            .outputMode("append")
            .trigger(availableNow=True)
            .option("checkpointLocation", checkpoint_path)
            .toTable(target_table)
        )

        query.awaitTermination()

        rec_count = spark.table(target_table).count()
        logger.log_success(records_processed=rec_count)

except Exception as e:
    logger.log_failure(e)
    raise e