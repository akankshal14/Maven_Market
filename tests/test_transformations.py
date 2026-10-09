import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# --- Transformation Logic Functions Under Test ---
def cleanse_customer_data(df):
    """Cleanses customer email addresses and handles null phone numbers."""
    return df.withColumn(
        "email_clean", when(col("email").like("%@%"), col("email")).otherwise("INVALID")
    ).withColumn(
        "phone_clean", coalesce(col("phone"), lit("UNKNOWN"))
    )


def compute_fact_sales_metrics(df):
    """Calculates total revenue, cost, profit, and profit margin percentage."""
    return (
        df.filter(col("quantity") > 0)
        .withColumn("total_revenue", col("quantity") * col("price"))
        .withColumn("total_cost", col("quantity") * col("unit_cost"))
        .withColumn("profit", col("total_revenue") - col("total_cost"))
        .withColumn(
            "profit_margin_pct",
            when(col("total_revenue") > 0, (col("profit") / col("total_revenue")) * 100.0).otherwise(0.0)
        )
    )


def aggregate_gold_sales_by_store(df):
    """Aggregates sales by store_id."""
    return (
        df.groupBy("store_id")
        .sum("total_revenue", "profit")
        .withColumnRenamed("sum(total_revenue)", "gross_revenue")
        .withColumnRenamed("sum(profit)", "total_profit")
    )


# --- Test Cases ---
def test_cleanse_customer_data(spark: SparkSession):
    """Test customer email cleansing and phone fallback."""
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True)
    ])
    data = [
        (1, "john@mavenmarket.com", "555-1234"),
        (2, "bad_email_format", None),
        (3, None, "555-5678")
    ]
    df = spark.createDataFrame(data, schema)
    
    result_df = cleanse_customer_data(df)
    results = {row["customer_id"]: row for row in result_df.collect()}
    
    assert results[1]["email_clean"] == "john@mavenmarket.com"
    assert results[1]["phone_clean"] == "555-1234"
    
    assert results[2]["email_clean"] == "INVALID"
    assert results[2]["phone_clean"] == "UNKNOWN"
    
    assert results[3]["email_clean"] == "INVALID"
    assert results[3]["phone_clean"] == "555-5678"


def test_compute_fact_sales_metrics_standard(spark: SparkSession):
    """Test sales metrics calculation for valid entries."""
    schema = StructType([
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("unit_cost", DoubleType(), True)
    ])
    data = [
        (2, 100.0, 60.0),  # Rev: 200, Cost: 120, Profit: 80, Margin: 40%
        (5, 20.0, 15.0)    # Rev: 100, Cost: 75, Profit: 25, Margin: 25%
    ]
    df = spark.createDataFrame(data, schema)
    
    res = compute_fact_sales_metrics(df).collect()
    
    assert res[0]["total_revenue"] == 200.0
    assert res[0]["total_cost"] == 120.0
    assert res[0]["profit"] == 80.0
    assert res[0]["profit_margin_pct"] == 40.0
    
    assert res[1]["total_revenue"] == 100.0
    assert res[1]["profit_margin_pct"] == 25.0


def test_compute_fact_sales_metrics_edge_cases(spark: SparkSession):
    """Test boundary conditions: negative/zero quantity and zero revenue."""
    schema = StructType([
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("unit_cost", DoubleType(), True)
    ])
    data = [
        (0, 50.0, 30.0),    # Quantity zero (should be filtered out)
        (-3, 50.0, 30.0),   # Negative quantity (should be filtered out)
        (1, 0.0, 10.0)      # Free item: Rev 0, Margin 0.0 (no division by zero)
    ]
    df = spark.createDataFrame(data, schema)
    
    res_df = compute_fact_sales_metrics(df)
    results = res_df.collect()
    
    # 2 rows with quantity <= 0 filtered out
    assert len(results) == 1
    assert results[0]["total_revenue"] == 0.0
    assert results[0]["profit_margin_pct"] == 0.0


def test_aggregate_gold_sales_by_store(load_fixture):
    """Test store aggregation using fixture sample data."""
    df = load_fixture("sample_transactions.json")
    
    # Filter valid quantity and calculate gross revenue
    valid_df = df.filter(col("quantity") > 0).withColumn(
        "total_revenue", col("quantity") * col("price")
    ).withColumn("profit", col("total_revenue") * 0.4)
    
    agg_df = aggregate_gold_sales_by_store(valid_df)
    results = {row["store_id"]: row for row in agg_df.collect()}
    
    # Store 10 has 2 valid transactions: (3*15.5 = 46.5) + (1*45.0 = 45.0) = 91.5
    assert 10 in results
    assert results[10]["gross_revenue"] == 91.5