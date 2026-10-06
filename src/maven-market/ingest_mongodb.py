import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from utils.custom_logger import CustomLogger

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

# Load Configuration
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

# Initialize Custom Logger
logger = CustomLogger(spark, "mongodb_products_ingestion")
logger.log_start()

try:
    # Retrieve Connection String from Azure Key Vault via Databricks Secret Scope
    mongo_uri = dbutils.secrets.get(
        scope=config["sources"]["mongodb"]["uri_secret_scope"], 
        key=config["sources"]["mongodb"]["uri_secret_key"]
    )

    # Load Product Master Data from MongoDB Atlas
    mongo_df = (spark.read
        .format("mongodb")
        .option("connection.uri", mongo_uri)
        .option("database", config["sources"]["mongodb"]["database"])
        .option("collection", config["sources"]["mongodb"]["collection"])
        .load())

    # Add Ingestion Metadata Audit Columns
    bronze_products = mongo_df \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source", lit("mongodb_atlas"))

    # Write Data to Bronze Delta Table
    bronze_products.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(config["tables"]["bronze_products"])

    logger.log_success(records_processed=bronze_products.count())

except Exception as e:
    logger.log_failure(e)
    raise e