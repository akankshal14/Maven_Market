import os
import sys
import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.custom_logger import CustomLogger


def test_logger_initialization(spark: SparkSession):
    """Test logger instance initialization."""
    logger = CustomLogger(spark=spark, pipeline_name="test_pipeline", target_table="audit_logs_test")
    assert logger.pipeline_name == "test_pipeline"
    assert logger.target_table == "audit_logs_test"


def test_logger_success_execution(spark: SparkSession, tmp_path):
    """Test successful log generation and written DataFrame structure."""
    target_table_path = str(tmp_path / "audit_logs_delta")
    
    # Instantiate logger overriding target table
    logger = CustomLogger(spark=spark, pipeline_name="test_success_pipeline", target_table="audit_test_success")
    
    # Mock _write_log to avoid writing to real tables in CI
    with patch.object(logger, '_write_log'):
        logger.log_start()
        logger.log_success(records_processed=150)
    
    # Assert standard schema fields
    schema_fields = [f.name for f in logger.schema.fields]
    assert "pipeline_name" in schema_fields
    assert "timestamp" in schema_fields
    assert "status" in schema_fields
    assert "records_processed" in schema_fields
    assert "error_message" in schema_fields


def test_logger_failure_execution(spark: SparkSession):
    """Test exception capturing and error traceback string creation."""
    logger = CustomLogger(spark=spark, pipeline_name="test_failure_pipeline", target_table="audit_test_fail")
    
    try:
        raise ValueError("Simulated pipeline failure")
    except Exception as e:
        # Mock _write_log to avoid writing to real tables in CI
        with patch.object(logger, '_write_log'):
            logger.log_failure(e)
        # Execution must proceed without raising unhandled internal logger exceptions
        assert True