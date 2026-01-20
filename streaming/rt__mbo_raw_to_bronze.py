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
dbutils.widgets.text("catalog", "spymaster")
dbutils.widgets.text("schema_name", "bronze")
dbutils.widgets.text("table_name", "mbo_stream")
dbutils.widgets.text("dlq_table_name", "mbo_stream_dlq")
dbutils.widgets.text("checkpoint_base", "abfss://lake@spymasterdevlakeoxxrlojs.dfs.core.windows.net/checkpoints")
dbutils.widgets.text("max_files_per_trigger", "1000")
dbutils.widgets.text("max_bytes_per_trigger", "1g")

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

EVENTHUB_CONNECTION_STRING = dbutils.secrets.get(scope="spymaster-runtime", key="eventhub-connection-string")

# COMMAND ----------

# Bronze envelope schema (matches data contract in WORK.md)
bronze_schema = StructType([
    StructField("event_time", LongType(), True),
    StructField("ingest_time", LongType(), True),
    StructField("venue", StringType(), True),
    StructField("symbol", IntegerType(), True),
    StructField("instrument_type", StringType(), True),
    StructField("underlier", StringType(), True),
    StructField("contract_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("order_id", LongType(), True),
    StructField("side", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("size", IntegerType(), True),
    StructField("sequence", LongType(), True),
    StructField("payload", StringType(), True),
])

# COMMAND ----------

# Event Hubs configuration
starting_position = json.dumps(
    {
        "seqNo": -1,
        "offset": "-1",
        "enqueuedTime": None,
        "isInclusive": True,
    }
)

ehConf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        f"{EVENTHUB_CONNECTION_STRING};EntityPath={EVENTHUB_NAME}"
    ),
    "eventhubs.consumerGroup": CONSUMER_GROUP,
    "eventhubs.startingPosition": starting_position,
}

# COMMAND ----------

# Read from Event Hubs
df_raw = (
    spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load()
)

# Parse the body as JSON with DLQ for malformed records
df_parsed = (
    df_raw
    .select(
        col("body").cast("string").alias("body_str"),
        from_json(col("body").cast("string"), bronze_schema).alias("data"),
        col("enqueuedTime").alias("eventhub_enqueued_time"),
        col("offset").alias("eventhub_offset"),
        col("sequenceNumber").alias("eventhub_sequence"),
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
