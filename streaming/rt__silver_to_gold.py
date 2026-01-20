# Databricks notebook source
# MAGIC %md
# MAGIC # rt__silver_to_gold
# MAGIC Gold streaming job: computes feature vectors from Silver bar_5s stream.
# MAGIC Outputs feature time-series and model input vectors.

# COMMAND ----------

import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, lag, avg, stddev, sum as spark_sum,
    array, lit, current_timestamp, to_json, struct, from_unixtime
)
from pyspark.sql.window import Window
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
dbutils.widgets.text("catalog", "spymaster")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("silver_table", "orderbook_5s")
dbutils.widgets.text("gold_schema", "gold")
dbutils.widgets.text("gold_table", "feature_vectors")
dbutils.widgets.text("eventhub_namespace", "ehnspymasterdevoxxrlojskvxey")
dbutils.widgets.text("eventhub_name", "features_gold")
dbutils.widgets.text("checkpoint_base", "abfss://lake@spymasterdevlakeoxxrlojs.dfs.core.windows.net/checkpoints")
dbutils.widgets.text("max_files_per_trigger", "1000")
dbutils.widgets.text("max_bytes_per_trigger", "1g")

CATALOG = dbutils.widgets.get("catalog")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
SILVER_TABLE = dbutils.widgets.get("silver_table")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")
GOLD_TABLE = dbutils.widgets.get("gold_table")
EVENTHUB_NAMESPACE = dbutils.widgets.get("eventhub_namespace")
EVENTHUB_NAME = dbutils.widgets.get("eventhub_name")
CHECKPOINT_BASE = dbutils.widgets.get("checkpoint_base")
MAX_FILES_PER_TRIGGER = int(dbutils.widgets.get("max_files_per_trigger"))
MAX_BYTES_PER_TRIGGER = dbutils.widgets.get("max_bytes_per_trigger")

SILVER_FULL_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.{SILVER_TABLE}"
GOLD_FULL_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.{GOLD_TABLE}"

EVENTHUB_CONNECTION_STRING = dbutils.secrets.get(scope="spymaster-runtime", key="eventhub-connection-string")

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
    .withColumn("bar_time_ts", from_unixtime(col("bar_ts") / lit(1_000_000_000)).cast("timestamp"))
    .withWatermark("bar_time_ts", "10 minutes")
)

logger.info("Silver stream loaded with watermark")

# COMMAND ----------

# Compute feature primitives using window functions
# This creates a streaming window over the bar_5s data

# Define window specs for lookback calculations with explicit ROWS BETWEEN for determinism
window_spec_5 = Window.partitionBy("contract_id").orderBy("bar_ts").rowsBetween(-4, 0)
window_spec_10 = Window.partitionBy("contract_id").orderBy("bar_ts").rowsBetween(-9, 0)
window_spec_20 = Window.partitionBy("contract_id").orderBy("bar_ts").rowsBetween(-19, 0)

df_features = (
    df_silver
    # Price returns
    .withColumn("prev_close", lag("close_price", 1).over(Window.partitionBy("contract_id").orderBy("bar_ts")))
    .withColumn("return_1", (col("close_price") - col("prev_close")) / col("prev_close"))

    # Moving averages
    .withColumn("ma_5", avg("close_price").over(window_spec_5))
    .withColumn("ma_10", avg("close_price").over(window_spec_10))
    .withColumn("ma_20", avg("close_price").over(window_spec_20))

    # Volatility
    .withColumn("vol_5", stddev("close_price").over(window_spec_5))
    .withColumn("vol_10", stddev("close_price").over(window_spec_10))

    # Volume features
    .withColumn("vol_ma_5", avg("volume").over(window_spec_5))
    .withColumn("vol_ratio", col("volume") / col("vol_ma_5"))

    # Spread features
    .withColumn("spread_ma_5", avg("spread").over(window_spec_5))
    .withColumn("spread_ratio", col("spread") / col("spread_ma_5"))

    # Price position relative to MAs
    .withColumn("price_vs_ma5", (col("close_price") - col("ma_5")) / col("ma_5"))
    .withColumn("price_vs_ma10", (col("close_price") - col("ma_10")) / col("ma_10"))

    # Momentum
    .withColumn("momentum_5", col("close_price") - lag("close_price", 5).over(Window.partitionBy("contract_id").orderBy("bar_ts")))
    .withColumn("momentum_10", col("close_price") - lag("close_price", 10).over(Window.partitionBy("contract_id").orderBy("bar_ts")))

    # Bid-ask imbalance
    .withColumn("bid_ask_imbalance", (col("bid_price") - col("ask_price")) / col("spread"))
)

# COMMAND ----------

# Create feature vector array
df_vectors = (
    df_features
    .select(
        col("contract_id"),
        col("bar_ts").alias("vector_time"),
        col("session_date"),
        col("close_price"),
        col("volume"),
        array(
            col("return_1"),
            col("price_vs_ma5"),
            col("price_vs_ma10"),
            col("vol_ratio"),
            col("spread_ratio"),
        ).alias("feature_vector"),
        # Also keep individual features for dashboard
        col("return_1"),
        col("ma_5"),
        col("ma_10"),
        col("vol_5"),
        col("vol_ratio"),
        col("spread"),
        col("spread_ratio"),
        col("momentum_5"),
        col("momentum_10"),
        col("bid_ask_imbalance"),
    )
    .withColumn("model_id", lit("ES_MODEL"))
    .withColumn("gold_ingest_time", current_timestamp())
    .na.fill(0.0)  # Handle nulls from window functions at stream start
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

ehConf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        f"{EVENTHUB_CONNECTION_STRING};EntityPath={EVENTHUB_NAME}"
    ),
}

df_to_eventhub = (
    df_vectors
    .select(
        to_json(struct(
            col("contract_id"),
            col("vector_time"),
            col("model_id"),
            col("close_price"),
            col("volume"),
            col("return_1"),
            col("ma_5"),
            col("vol_ratio"),
            col("spread"),
            col("momentum_5"),
            col("bid_ask_imbalance"),
        )).alias("body")
    )
)

query_eventhub = (
    df_to_eventhub.writeStream
    .format("eventhubs")
    .options(**ehConf)
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
