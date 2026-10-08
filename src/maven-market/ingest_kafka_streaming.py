import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from utils.custom_logger import CustomLogger

# 1. Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

# 2. Load YAML Configuration
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

# 3. Initialize Operational Logger
logger = CustomLogger(spark, "kafka_orders_streaming")
logger.log_start()

try:
    # 4. Fetch Kafka Secrets from Key Vault Scope ('maven_keyvault_scope')
    kafka_key = dbutils.secrets.get(
        scope=config["sources"]["kafka"]["secret_scope"], 
        key=config["sources"]["kafka"]["api_key_name"]
    )
    kafka_secret = dbutils.secrets.get(
        scope=config["sources"]["kafka"]["secret_scope"], 
        key=config["sources"]["kafka"]["api_secret_name"]
    )

    # 5. Build SASL/JAAS Authentication Configuration String
    jaas_config = (
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{kafka_key}" password="{kafka_secret}";'
    )

    # 6. Read Real-Time Stream from Confluent Kafka
    kafka_stream_df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config["sources"]["kafka"]["bootstrap_servers"])
        .option("subscribe", config["sources"]["kafka"]["topic"])
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("startingOffsets", "earliest")
        .load())

    # 7. Deserialize Binary Payload & Append Technical Metadata
    bronze_kafka_df = (kafka_stream_df
        .selectExpr(
            "CAST(key AS STRING) as order_key", 
            "CAST(value AS STRING) as payload", 
            "topic", 
            "partition", 
            "offset"
        )
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("confluent_kafka"))
    )

    # 8. Sink Stream to Unity Catalog Delta Table with ADLS Checkpointing
    query = (bronze_kafka_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", config["sources"]["kafka"]["checkpoint_location"])
        .toTable(config["tables"]["bronze_kafka_orders"]))

    # 9. Execution Window for Testing/POC (Timeout after 60s)
    query.awaitTermination(timeout=60)

    # 10. Record Metrics to Logs
    rec_count = spark.table(config["tables"]["bronze_kafka_orders"]).count()
    logger.log_success(records_processed=rec_count)

except Exception as e:
    logger.log_failure(e)
    raise e




