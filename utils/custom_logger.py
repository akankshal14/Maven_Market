import logging
import traceback
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

class CustomLogger:
    def __init__(self, spark: SparkSession, pipeline_name: str, target_table: str = "maven_market_uc.audit.audit_logs"):
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.target_table = target_table
        
        # Define schema for centralized logging table
        self.schema = StructType([
            StructField("pipeline_name", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("records_processed", LongType(), True),
            StructField("error_message", StringType(), True)
        ])

    def _write_log(self, status: str, records_processed: int = 0, error_message: str = None):
        log_data = [(
            self.pipeline_name,
            datetime.now(),
            status,
            records_processed,
            error_message
        )]
        
        log_df = self.spark.createDataFrame(log_data, self.schema)
        log_df.write.format("delta").mode("append").saveAsTable(self.target_table)

    def log_start(self):
        print(f"[INFO] Starting pipeline execution: {self.pipeline_name}")
        self._write_log(status="STARTED")

    def log_success(self, records_processed: int):
        print(f"[SUCCESS] Pipeline {self.pipeline_name} completed. Processed {records_processed} records.")
        self._write_log(status="SUCCESS", records_processed=records_processed)

    def log_failure(self, error: Exception):
        err_msg = "".join(traceback.format_exception(type(error), error, error.__traceback__))
        print(f"[ERROR] Pipeline {self.pipeline_name} failed:\n{err_msg}")
        self._write_log(status="FAILED", error_message=err_msg)