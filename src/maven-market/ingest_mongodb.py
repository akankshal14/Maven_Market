import os
import sys
import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType
)

# -------------------------------------------------------------------------
# Schema Definitions
# -------------------------------------------------------------------------

# Schema for Customers Collection
customers_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("customer_acct_num", LongType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("customer_address", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state_province", StringType(), True),
    StructField("customer_postal_code", IntegerType(), True),
    StructField("customer_country", StringType(), True),
    StructField("birthdate", StringType(), True),
    StructField("marital_status", StringType(), True),
    StructField("yearly_income", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("total_children", IntegerType(), True),
    StructField("num_children_at_home", IntegerType(), True),
    StructField("education", StringType(), True),
    StructField("acct_open_date", StringType(), True),
    StructField("member_card", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("homeowner", StringType(), True)
])

# Schema for Products Collection
products_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_brand", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_sku", LongType(), True),
    StructField("product_retail_price", DoubleType(), True),
    StructField("product_cost", DoubleType(), True),
    StructField("product_weight", DoubleType(), True),
    StructField("recyclable", IntegerType(), True),
    StructField("low_fat", IntegerType(), True)
])

# Mapping collections to their respective Spark schemas
collection_schemas = {
    "customers": customers_schema,
    "products": products_schema
}

# -------------------------------------------------------------------------
# Helper Functions & System Setup
# -------------------------------------------------------------------------

def locate_config():
    """Traverse system path and parent directories to locate config.yml."""
    for path_entry in [os.getcwd()] + sys.path:
        if not path_entry or not os.path.exists(path_entry):
            continue
        curr = os.path.abspath(path_entry)
        for _ in range(5):  # Traverse up to 5 directory levels
            candidate = os.path.join(curr, "config.yml")
            if os.path.exists(candidate):
                return candidate
            parent = os.path.dirname(curr)
            if parent == curr:
                break
            curr = parent
    return None

config_path = locate_config()

if not config_path:
    raise FileNotFoundError(
        f"config.yml not found. Current working directory: {os.getcwd()}"
    )

# Set project root so Python can import custom modules like 'utils'
project_root = os.path.dirname(config_path)
if project_root not in sys.path:
    sys.path.append(project_root)

# Import custom logger after updating sys.path
from utils.custom_logger import CustomLogger

# Retrieve or create Spark Session
spark = SparkSession.builder.getOrCreate()

# Load Configuration
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# Initialize Custom Logger
logger = CustomLogger(spark, "mongodb_batch_ingestion")
logger.log_start()

# -------------------------------------------------------------------------
# Main Execution Pipeline
# -------------------------------------------------------------------------

try:
    # Retrieve Connection String from Secret Scope
    mongo_uri = dbutils.secrets.get(
        scope=config["sources"]["mongodb"]["uri_secret_scope"],
        key=config["sources"]["mongodb"]["uri_secret_key"]
    )
    
    database_name = config["sources"]["mongodb"]["database"]
    mongo_collections = config["sources"]["mongodb"].get("collections", [
        {"name": "products", "table_key": "bronze_products"},
        {"name": "customers", "table_key": "bronze_customers"}
    ])

    total_records = 0

    # Process collections sequentially
    for item in mongo_collections:
        coll_name = item["name"]
        table_key = item["table_key"]
        target_table = config["tables"][table_key]

        # Configure base MongoDB Spark reader
        reader = (spark.read
            .format("mongodb")
            .option("connection.uri", mongo_uri)
            .option("database", database_name)
            .option("collection", coll_name))

        # Apply schema explicitly if defined; fall back to sampling if unknown
        if coll_name in collection_schemas:
            reader = reader.schema(collection_schemas[coll_name])
        else:
            reader = reader.option("sampleSize", 10000)

        mongo_df = reader.load()

        # Append audit metadata columns
        bronze_df = mongo_df \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source", lit(f"mongodb_{coll_name}"))

        # Write data to Bronze Delta Table
        bronze_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table)

        rec_count = bronze_df.count()
        total_records += rec_count

    logger.log_success(records_processed=total_records)

except Exception as e:
    logger.log_failure(e)
    raise e