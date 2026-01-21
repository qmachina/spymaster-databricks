# Databricks notebook source
# MAGIC %md
# MAGIC # rt__mbo_raw_to_bronze
# MAGIC Bronze streaming job: reads from Event Hubs `mbo_raw` and writes to Delta lake.

# COMMAND ----------

import json
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_date, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, IntegerType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# Configure Spark for RocksDB state store and optimizations
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.databricks.adaptive.autoOptimizeShuffle.enabled", "true")

logger.info("Spark configuration complete")

# COMMAND ----------

# Configuration from widgets (for parameterization)
dbutils.widgets.text("eventhub_namespace", "ehnspymasterdevoxxrlojskvxey")
dbutils.widgets.text("eventhub_name", "mbo_raw")
dbutils.widgets.text("consumer_group", "databricks_bronze")
dbutils.widgets.text("catalog", "bronze")
dbutils.widgets.text("schema_name", "default")
dbutils.widgets.text("table_name", "mbo_stream")
dbutils.widgets.text("dlq_table_name", "mbo_stream_dlq")
dbutils.widgets.text("checkpoint_base", "abfss://lake@spymasterdevlakeoxxrlojs.dfs.core.windows.net/checkpoints")
dbutils.widgets.text("max_files_per_trigger", "1000")
dbutils.widgets.text("max_bytes_per_trigger", "1g")
dbutils.widgets.text("max_offsets_per_trigger", "50000")
dbutils.widgets.text("kafka_request_timeout_ms", "60000")
dbutils.widgets.text("kafka_session_timeout_ms", "30000")

EVENTHUB_NAMESPACE = dbutils.widgets.get("eventhub_namespace")
EVENTHUB_NAME = dbutils.widgets.get("eventhub_name")
CONSUMER_GROUP = dbutils.widgets.get("consumer_group")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
TABLE_NAME = dbutils.widgets.get("table_name")
DLQ_TABLE_NAME = dbutils.widgets.get("dlq_table_name")
CHECKPOINT_BASE = dbutils.widgets.get("checkpoint_base")
MAX_FILES_PER_TRIGGER = int(dbutils.widgets.get("max_files_per_trigger"))
MAX_BYTES_PER_TRIGGER = dbutils.widgets.get("max_bytes_per_trigger")
MAX_OFFSETS_PER_TRIGGER = int(dbutils.widgets.get("max_offsets_per_trigger"))
KAFKA_REQUEST_TIMEOUT_MS = dbutils.widgets.get("kafka_request_timeout_ms")
KAFKA_SESSION_TIMEOUT_MS = dbutils.widgets.get("kafka_session_timeout_ms")

EVENTHUB_CONNECTION_STRING = dbutils.secrets.get(scope="spymaster", key="eventhub-connection-string")

# COMMAND ----------

# Bronze envelope schema (matches data contract in WORK.md)
bronze_schema = StructType([
    StructField("event_time", LongType(), True),
    StructField("ingest_time", LongType(), True),
    StructField("venue", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("instrument_type", StringType(), True),
    StructField("underlier", StringType(), True),
    StructField("contract_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("order_id", LongType(), True),
    StructField("side", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("size", LongType(), True),
    StructField("sequence", LongType(), True),
    StructField("payload", StringType(), True),
])

# COMMAND ----------

# Event Hubs Kafka configuration
if not EVENTHUB_NAMESPACE:
    raise ValueError("eventhub_namespace is required")
if not EVENTHUB_NAME:
    raise ValueError("eventhub_name is required")
if not EVENTHUB_CONNECTION_STRING:
    raise ValueError("eventhub-connection-string is required")

eventhub_conn_str = EVENTHUB_CONNECTION_STRING.split(";EntityPath=")[0].strip()
if not eventhub_conn_str:
    raise ValueError("eventhub-connection-string is empty")

kafka_options = {
    "kafka.bootstrap.servers": f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EVENTHUB_NAME,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="$ConnectionString" password="{eventhub_conn_str}";'
    ),
    "startingOffsets": "latest",
    "kafka.group.id": CONSUMER_GROUP,
    "maxOffsetsPerTrigger": str(MAX_OFFSETS_PER_TRIGGER),
    "kafka.request.timeout.ms": KAFKA_REQUEST_TIMEOUT_MS,
    "kafka.session.timeout.ms": KAFKA_SESSION_TIMEOUT_MS,
}

# COMMAND ----------

# Read from Event Hubs
df_raw = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

# Parse the body as JSON with DLQ for malformed records
df_parsed = (
    df_raw
    .select(
        col("value").cast("string").alias("body_str"),
        from_json(col("value").cast("string"), bronze_schema).alias("data"),
        col("timestamp").alias("eventhub_enqueued_time"),
        col("offset").cast("string").alias("eventhub_offset"),
        col("offset").alias("eventhub_sequence"),
        col("partition").alias("eventhub_partition")
    )
    .withColumn("bronze_ingest_time", current_timestamp())
)

# Split into valid and invalid (DLQ) records
df_valid = df_parsed.filter(col("data").isNotNull())
df_invalid = df_parsed.filter(col("data").isNull())

# Process valid records with watermark and deduplication
df_bronze = (
    df_valid
    .select("data.*", "eventhub_enqueued_time", "eventhub_offset", "eventhub_sequence")
    .withColumn("bronze_ingest_time", current_timestamp())
    .withColumn("session_date", to_date(col("eventhub_enqueued_time")))
    .withWatermark("eventhub_enqueued_time", "10 minutes")
    .dropDuplicatesWithinWatermark(["contract_id", "order_id", "event_time"])
)

# Prepare DLQ records
df_dlq = (
    df_invalid
    .select(
        col("body_str").alias("raw_record"),
        lit("JSON parse failure").alias("error_message"),
        col("eventhub_partition").alias("source_partition"),
        col("eventhub_offset").alias("source_offset"),
        col("bronze_ingest_time").alias("ingestion_timestamp"),
        lit(0).cast("int").alias("retry_count")
    )
)

# COMMAND ----------

# Write valid records to Unity Catalog managed table with rate limiting
full_table_name = f"{CATALOG}.{SCHEMA_NAME}.{TABLE_NAME}"
print(f"Writing valid records to: {full_table_name}")

query_main = (
    df_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/rt__bronze_main")
    .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
    .option("maxBytesPerTrigger", MAX_BYTES_PER_TRIGGER)
    .trigger(processingTime="10 seconds")
    .toTable(full_table_name)
)

# COMMAND ----------

# Write invalid records to DLQ table
dlq_full_table_name = f"{CATALOG}.{SCHEMA_NAME}.{DLQ_TABLE_NAME}"
print(f"Writing invalid records to DLQ: {dlq_full_table_name}")

query_dlq = (
    df_dlq.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/rt__bronze_dlq")
    .trigger(processingTime="10 seconds")
    .toTable(dlq_full_table_name)
)

# COMMAND ----------

# Monitor both streams
print("Streaming queries started. Monitoring progress...")
import json

def log_progress():
    if query_main.isActive:
        progress = query_main.lastProgress
        if progress:
            print(f"Main query progress: {json.dumps(progress, indent=2)}")
    
    if query_dlq.isActive:
        progress = query_dlq.lastProgress
        if progress:
            print(f"DLQ query progress: {json.dumps(progress, indent=2)}")

# Wait for termination (runs indefinitely)
spark.streams.awaitAnyTermination()
