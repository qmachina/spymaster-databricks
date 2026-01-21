# Databricks notebook source
# MAGIC %md
# MAGIC # rt__silver_to_gold
# MAGIC Gold streaming job: computes feature vectors from Silver bar_5s stream.
# MAGIC Outputs feature time-series and model input vectors.

# COMMAND ----------

import logging
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, array, lit, current_timestamp, to_json, struct
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
    IntegerType, ArrayType, TimestampType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# Configure Spark for optimizations
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.databricks.adaptive.autoOptimizeShuffle.enabled", "true")

logger.info("Spark configuration complete")

# COMMAND ----------

# Configuration from widgets
dbutils.widgets.text("catalog_silver", "silver")
dbutils.widgets.text("silver_schema", "default")
dbutils.widgets.text("silver_table", "feature_primitives")
dbutils.widgets.text("catalog_gold", "gold")
dbutils.widgets.text("gold_schema", "default")
dbutils.widgets.text("gold_table", "feature_vectors")
dbutils.widgets.text("eventhub_namespace", "ehnspymasterdevoxxrlojskvxey")
dbutils.widgets.text("eventhub_name", "features_gold")
dbutils.widgets.text("checkpoint_base", "abfss://lake@spymasterdevlakeoxxrlojs.dfs.core.windows.net/checkpoints")
dbutils.widgets.text("max_files_per_trigger", "1000")
dbutils.widgets.text("max_bytes_per_trigger", "1g")

CATALOG_SILVER = dbutils.widgets.get("catalog_silver")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
SILVER_TABLE = dbutils.widgets.get("silver_table")
CATALOG_GOLD = dbutils.widgets.get("catalog_gold")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")
GOLD_TABLE = dbutils.widgets.get("gold_table")
EVENTHUB_NAMESPACE = dbutils.widgets.get("eventhub_namespace")
EVENTHUB_NAME = dbutils.widgets.get("eventhub_name")
CHECKPOINT_BASE = dbutils.widgets.get("checkpoint_base")
MAX_FILES_PER_TRIGGER = int(dbutils.widgets.get("max_files_per_trigger"))
MAX_BYTES_PER_TRIGGER = dbutils.widgets.get("max_bytes_per_trigger")

SILVER_FULL_TABLE = f"{CATALOG_SILVER}.{SILVER_SCHEMA}.{SILVER_TABLE}"
GOLD_FULL_TABLE = f"{CATALOG_GOLD}.{GOLD_SCHEMA}.{GOLD_TABLE}"

EVENTHUB_CONNECTION_STRING = dbutils.secrets.get(scope="spymaster", key="eventhub-connection-string")

# COMMAND ----------

# Read from Silver Unity Catalog table with rate limiting
logger.info(f"Reading from silver table: {SILVER_FULL_TABLE}")

df_silver = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
    .option("maxBytesPerTrigger", MAX_BYTES_PER_TRIGGER)
    .table(SILVER_FULL_TABLE)
)

# Add timestamp column for watermarking
df_silver = (
    df_silver
    .withWatermark("bucket_ts", "10 minutes")
)

logger.info("Silver stream loaded with watermark")

# COMMAND ----------

state_schema = StructType([
    StructField("last_mid", DoubleType(), True),
])

output_schema = StructType([
    StructField("contract_id", StringType(), True),
    StructField("vector_time", LongType(), True),
    StructField("session_date", StringType(), True),
    StructField("underlier", StringType(), True),
    StructField("instrument_type", StringType(), True),
    StructField("ofi_ratio", DoubleType(), True),
    StructField("spread", DoubleType(), True),
    StructField("mid_change", DoubleType(), True),
    StructField("vwap", DoubleType(), True),
    StructField("event_count", LongType(), True),
    StructField("feature_vector", ArrayType(DoubleType()), True),
])


def build_feature_vectors(
    key,
    pdf_iter,
    state: GroupState,
):
    contract_id = key[0]
    last_mid = None
    if state.exists:
        last_mid = state.get["last_mid"]

    outputs = []
    for pdf in pdf_iter:
        if pdf.empty:
            continue
        pdf = pdf.sort_values("bucket_ns")
        prev_mid = pdf["mid"].shift(1)
        if last_mid is not None:
            prev_mid.iloc[0] = last_mid
        else:
            prev_mid.iloc[0] = pdf["mid"].iloc[0]
        pdf["mid_change"] = pdf["mid"] - prev_mid

        for _, row in pdf.iterrows():
            feature_vector = [
                float(row["ofi_ratio"]),
                float(row["spread"]),
                float(row["mid_change"]),
                float(row["vwap"]),
                float(row["event_count"]),
            ]
            outputs.append(
                {
                    "contract_id": contract_id,
                    "vector_time": int(row["bucket_ns"]),
                    "session_date": str(row["session_date"]),
                    "underlier": str(row["underlier"]),
                    "instrument_type": str(row["instrument_type"]),
                    "ofi_ratio": float(row["ofi_ratio"]),
                    "spread": float(row["spread"]),
                    "mid_change": float(row["mid_change"]),
                    "vwap": float(row["vwap"]),
                    "event_count": int(row["event_count"]),
                    "feature_vector": feature_vector,
                }
            )

        last_mid = float(pdf["mid"].iloc[-1])

    if last_mid is not None:
        state.update({"last_mid": last_mid})
        state.setTimeoutDuration(600000)

    if outputs:
        yield pd.DataFrame(outputs)
    else:
        yield pd.DataFrame(columns=output_schema.fieldNames())


df_vectors = (
    df_silver
    .groupBy("contract_id")
    .applyInPandasWithState(
        build_feature_vectors,
        outputStructType=output_schema,
        stateStructType=state_schema,
        outputMode="append",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
    )
    .withColumn("model_id", lit("ES_MODEL"))
    .withColumn("gold_ingest_time", current_timestamp())
)

# COMMAND ----------

# Write to Gold Unity Catalog managed table
logger.info(f"Writing to gold table: {GOLD_FULL_TABLE}")

query_vectors = (
    df_vectors.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/rt__gold_delta")
    .trigger(processingTime="10 seconds")
    .toTable(GOLD_FULL_TABLE)
)

logger.info("Gold Delta write query started")

# COMMAND ----------

# Publish to Event Hubs for Fabric ingestion
logger.info(f"Publishing to Event Hub: {EVENTHUB_NAMESPACE}/{EVENTHUB_NAME}")

if not EVENTHUB_NAMESPACE:
    raise ValueError("eventhub_namespace is required")
if not EVENTHUB_NAME:
    raise ValueError("eventhub_name is required")
if not EVENTHUB_CONNECTION_STRING:
    raise ValueError("eventhub-connection-string is required")

eventhub_conn_str = EVENTHUB_CONNECTION_STRING.split(";EntityPath=")[0].strip()
if not eventhub_conn_str:
    raise ValueError("eventhub-connection-string is empty")

kafka_write_options = {
    "kafka.bootstrap.servers": f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093",
    "topic": EVENTHUB_NAME,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="$ConnectionString" password="{eventhub_conn_str}";'
    ),
}

df_to_eventhub = (
    df_vectors
    .select(
        to_json(struct(
            col("contract_id"),
            col("vector_time"),
            col("model_id"),
            col("ofi_ratio"),
            col("spread"),
            col("mid_change"),
            col("vwap"),
            col("event_count"),
        )).alias("value")
    )
)

query_eventhub = (
    df_to_eventhub.writeStream
    .format("kafka")
    .options(**kafka_write_options)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/rt__gold_eventhub")
    .trigger(processingTime="10 seconds")
    .start()
)

# COMMAND ----------

# Monitor both streams
logger.info("Streaming queries started. Monitoring progress...")

def log_progress():
    if query_vectors.isActive:
        progress = query_vectors.lastProgress
        if progress:
            logger.info(f"Delta query progress: {json.dumps(progress, indent=2)}")
    
    if query_eventhub.isActive:
        progress = query_eventhub.lastProgress
        if progress:
            logger.info(f"EventHub query progress: {json.dumps(progress, indent=2)}")

# Wait for both queries
spark.streams.awaitAnyTermination()
