import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.config_loader import load_config


def test_dlt_expectations_against_dataframe(spark: SparkSession):
    """Verify DLT expectation string SQL expressions against sample records."""
    config = load_config()
    expectations = config.get("expectations", {})
    
    tx_rule = expectations["transactions"]["valid_quantity"]  # "quantity > 0"
    email_rule = expectations["customers"]["valid_email"]    # "email LIKE '%@%'"
    
    # Test Data
    data = [
        (1, 5, "alice@mavenmarket.com"),
        (2, -1, "bob_invalid_email"),
        (3, 0, "carol@mavenmarket.com")
    ]
    df = spark.createDataFrame(data, ["id", "quantity", "email"])
    
    # Apply SQL Expectation Filters
    valid_tx_df = df.filter(expr(tx_rule))
    valid_email_df = df.filter(expr(email_rule))
    
    assert valid_tx_df.count() == 1  # Only row id=1 has quantity > 0
    assert valid_email_df.count() == 2  # Rows id=1 and id=3 have valid email format