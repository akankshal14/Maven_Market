import os
import dlt
import yaml
import pyspark.sql.functions as F

# 1. Dynamically locate config.yml relative to this script
try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.getcwd()

_repo_root = _script_dir
while _repo_root != os.path.dirname(_repo_root):
    if os.path.exists(os.path.join(_repo_root, "config.yml")):
        break
    _repo_root = os.path.dirname(_repo_root)

config_path = os.path.join(_repo_root, "config.yml")

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# Extract dynamic expectations from config
exp_orders = config.get("expectations", {}).get("orders", {})
exp_transactions = config.get("expectations", {}).get("transactions", {})
exp_inventory = config.get("expectations", {}).get("inventory", {})

# Helper to get full table paths safely
def get_table_path(key, default_name):
    base_catalog = config.get("catalog", "maven_market_uc")
    path = config.get("tables", {}).get(key, f"{base_catalog}.bronze.{default_name}")
    return path.replace("${catalog}", base_catalog)

# -------------------------------------------------------------------
# SILVER LAYER: Cleansing & Data Quality
# -------------------------------------------------------------------

@dlt.table(name="maven_market_uc.silver.silver_orders", comment="Cleansed Kafka orders with validated expectations.")
@dlt.expect_all(exp_orders)
def silver_orders():
    bronze_table = get_table_path("bronze_kafka_orders", "raw_kafka_orders")
    return (
        spark.readStream.table(bronze_table)
        .withColumn("transaction_date", F.to_date(F.col("transaction_date"), "yyyy-MM-dd"))
        .withColumn("stock_date", F.to_date(F.col("stock_date"), "yyyy-MM-dd"))
        .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
        .drop("_source", "_ingested_at")
    )

@dlt.table(name="maven_market_uc.silver.silver_inventory", comment="Cleansed Kafka inventory updates.")
@dlt.expect_all(exp_inventory)
def silver_inventory():
    bronze_table = get_table_path("bronze_kafka_inventory", "raw_kafka_inventory")
    return (
        spark.readStream.table(bronze_table)
        .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
        .drop("_source", "_ingested_at")
    )

@dlt.table(name="maven_market_uc.silver.silver_transactions", comment="Cleansed batch transactions.")
@dlt.expect_all(exp_transactions)
def silver_transactions():
    bronze_table = get_table_path("bronze_transactions", "raw_transactions")
    return (
        spark.readStream.table(bronze_table)
        .drop("_source_file", "_ingested_at")
    )

@dlt.table(name="maven_market_uc.silver.silver_products", comment="Cleansed product master data.")
def silver_products():
    bronze_table = get_table_path("bronze_products", "raw_products")
    return spark.readStream.table(bronze_table).drop("_source", "_ingested_at")

@dlt.table(name="maven_market_uc.silver.silver_stores", comment="Cleansed store dimension data.")
def silver_stores():
    bronze_table = "maven_market_uc.bronze.stores"
    return spark.readStream.table(bronze_table).drop("_source_file", "_ingested_at")

@dlt.table(name="maven_market_uc.silver.silver_calendar", comment="Cleansed calendar dimension data.")
def silver_calendar():
    bronze_table = "maven_market_uc.bronze.calender"
    return spark.readStream.table(bronze_table).drop("_source_file", "_ingested_at")

# -------------------------------------------------------------------
# SILVER LAYER: SCD Type 2 Implementation
# -------------------------------------------------------------------

dlt.create_streaming_table(name="maven_market_uc.silver.silver_customers_scd2")

dlt.apply_changes(
    target="maven_market_uc.silver.silver_customers_scd2",
    source=get_table_path("bronze_customers", "raw_customers"),
    keys=["customer_id"],
    sequence_by=F.col("_ingested_at"),
    apply_as_deletes=None,
    except_column_list=["_source", "_ingested_at"],
    stored_as_scd_type=2
)

# -------------------------------------------------------------------
# GOLD LAYER: Aggregated Star Schema
# -------------------------------------------------------------------

@dlt.table(name="maven_market_uc.gold.gold_dim_customers", comment="Gold dimension for Customers (Current State).")
def gold_dim_customers():
    # Read using the multipart name defined in the Silver layer
    return dlt.read("maven_market_uc.silver.silver_customers_scd2").filter(F.col("__END_AT").isNull())

@dlt.table(name="maven_market_uc.gold.gold_dim_products", comment="Gold dimension for Products.")
def gold_dim_products():
    return dlt.read("maven_market_uc.silver.silver_products")

@dlt.table(name="maven_market_uc.gold.gold_dim_stores", comment="Gold dimension for Stores.")
def gold_dim_stores():
    return dlt.read("maven_market_uc.silver.silver_stores")

@dlt.table(name="maven_market_uc.gold.gold_fact_sales", comment="Unified and aggregated Fact table optimized for BI.")
def gold_fact_sales():
    transactions = dlt.read("maven_market_uc.silver.silver_transactions").select(
        F.col("transaction_date"),
        F.col("customer_id"),
        F.col("store_id"),
        F.col("product_id"),
        F.col("quantity"),
        # Dynamically handle price column if it exists in POS transactions
        F.col("product_price").alias("unit_price") if "product_price" in dlt.read("maven_market_uc.silver.silver_transactions").columns else F.lit(0.0).alias("unit_price")
    )
    
    orders = dlt.read("maven_market_uc.silver.silver_orders").select(
        F.col("transaction_date"),
        F.col("customer_id"),
        F.col("store_id"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("unit_price")
    )
    
    # Union Batch POS Transactions and Real-Time Kafka Orders
    unified_sales = transactions.unionByName(orders, allowMissingColumns=True)
    
    return (
        unified_sales.groupBy("transaction_date", "store_id", "product_id")
        .agg(
            F.sum("quantity").alias("total_quantity_sold"),
            F.sum(F.col("quantity") * F.col("unit_price")).alias("total_revenue"),
            F.count("*").alias("transaction_count")
        )
    )