# Databricks notebook source
# MAGIC %md
# MAGIC # rt__gold_to_inference
# MAGIC Inference streaming job: calls Azure ML endpoint with feature vectors.
# MAGIC Publishes scores to Event Hub for Fabric dashboard consumption.

# COMMAND ----------

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, pandas_udf, struct, to_json, current_timestamp, lit, from_unixtime
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
    IntegerType, ArrayType, TimestampType
)
import pandas as pd
import requests
import json
from typing import Iterator

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
dbutils.widgets.text("gold_schema", "gold")
dbutils.widgets.text("gold_table", "feature_vectors")
dbutils.widgets.text("inference_schema", "gold")
dbutils.widgets.text("inference_table", "inference_scores")
dbutils.widgets.text("eventhub_namespace", "ehnspymasterdevoxxrlojskvxey")
dbutils.widgets.text("eventhub_name", "inference_scores")
dbutils.widgets.text("checkpoint_base", "abfss://lake@spymasterdevlakeoxxrlojs.dfs.core.windows.net/checkpoints")
dbutils.widgets.text("max_files_per_trigger", "500")
dbutils.widgets.text("max_bytes_per_trigger", "512m")
dbutils.widgets.text("aml_endpoint_uri", "https://es-model-endpoint.westus.inference.ml.azure.com/score")

CATALOG = dbutils.widgets.get("catalog")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")
GOLD_TABLE = dbutils.widgets.get("gold_table")
INFERENCE_SCHEMA = dbutils.widgets.get("inference_schema")
INFERENCE_TABLE = dbutils.widgets.get("inference_table")
EVENTHUB_NAMESPACE = dbutils.widgets.get("eventhub_namespace")
EVENTHUB_NAME = dbutils.widgets.get("eventhub_name")
CHECKPOINT_BASE = dbutils.widgets.get("checkpoint_base")
MAX_FILES_PER_TRIGGER = int(dbutils.widgets.get("max_files_per_trigger"))
MAX_BYTES_PER_TRIGGER = dbutils.widgets.get("max_bytes_per_trigger")
AML_ENDPOINT_URI = dbutils.widgets.get("aml_endpoint_uri")

GOLD_FULL_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.{GOLD_TABLE}"
INFERENCE_FULL_TABLE = f"{CATALOG}.{INFERENCE_SCHEMA}.{INFERENCE_TABLE}"

AML_API_KEY = dbutils.secrets.get(scope="spymaster-runtime", key="aml-endpoint-key")
EVENTHUB_CONNECTION_STRING = dbutils.secrets.get(scope="spymaster-runtime", key="eventhub-connection-string")

# COMMAND ----------

# Define inference UDF with retry logic and proper error handling
@pandas_udf("struct<prediction:int, prob_0:double, prob_1:double, error_message:string>")
def call_aml_endpoint(feature_vectors: pd.Series) -> pd.DataFrame:
    """
    Batch inference UDF that calls Azure ML managed online endpoint with retries.
    """
    results = []

    # Convert Series of arrays to list of lists
    vectors = [list(v) if v is not None else [0.0] * 5 for v in feature_vectors]

    # Batch the requests (Azure ML supports batch inference)
    batch_size = 100
    max_retries = 3
    
    for i in range(0, len(vectors), batch_size):
        batch = vectors[i:i + batch_size]
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                response = requests.post(
                    AML_ENDPOINT_URI,
                    headers={
                        "Authorization": f"Bearer {AML_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={"features": batch},
                    timeout=10,
                )
                response.raise_for_status()
                result = response.json()

                # Parse response
                if isinstance(result, str):
                    result = json.loads(result)

                predictions = result.get("predictions", [])
                probabilities = result.get("probabilities", [])

                for pred, probs in zip(predictions, probabilities):
                    results.append({
                        "prediction": int(pred),
                        "prob_0": float(probs[0]) if len(probs) > 0 else 0.0,
                        "prob_1": float(probs[1]) if len(probs) > 1 else 0.0,
                        "error_message": None,
                    })
                
                success = True

            except requests.exceptions.Timeout as e:
                retry_count += 1
                error_msg = f"Timeout on attempt {retry_count}/{max_retries}: {str(e)}"
                logger.warning(error_msg)
                if retry_count == max_retries:
                    for _ in batch:
                        results.append({
                            "prediction": -1,
                            "prob_0": 0.0,
                            "prob_1": 0.0,
                            "error_message": error_msg,
                        })
            
            except Exception as e:
                retry_count += 1
                error_msg = f"Error on attempt {retry_count}/{max_retries}: {str(e)}"
                logger.error(error_msg)
                if retry_count == max_retries:
                    for _ in batch:
                        results.append({
                            "prediction": -1,
                            "prob_0": 0.0,
                            "prob_1": 0.0,
                            "error_message": error_msg,
                        })

    return pd.DataFrame(results)

# COMMAND ----------

# Read from Gold Unity Catalog table with rate limiting and watermark
logger.info(f"Reading from gold table: {GOLD_FULL_TABLE}")

df_gold = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
    .option("maxBytesPerTrigger", MAX_BYTES_PER_TRIGGER)
    .table(GOLD_FULL_TABLE)
)

# Add watermark if vector_time is timestamp, otherwise convert it
df_gold = (
    df_gold
    .withColumn("vector_timestamp", from_unixtime(col("vector_time") / lit(1_000_000_000)).cast("timestamp"))
    .withWatermark("vector_timestamp", "10 minutes")
)

logger.info("Gold stream loaded with watermark")

# COMMAND ----------

# Apply inference with error tracking
logger.info("Applying inference UDF...")

df_with_scores = (
    df_gold
    .withColumn("inference_result", call_aml_endpoint(col("feature_vector")))
    .select(
        col("contract_id"),
        col("vector_time"),
        col("session_date") if "session_date" in df_gold.columns else lit(None).alias("session_date"),
        col("model_id") if "model_id" in df_gold.columns else lit("ES_MODEL").alias("model_id"),
        col("close_price") if "close_price" in df_gold.columns else lit(None).alias("close_price"),
        col("feature_vector"),
        col("inference_result.prediction").alias("prediction"),
        col("inference_result.prob_0").alias("prob_0"),
        col("inference_result.prob_1").alias("prob_1"),
        col("inference_result.error_message").alias("inference_error"),
    )
    .withColumn("inference_time", current_timestamp())
    .withColumn("model_version", lit("1"))
)

# COMMAND ----------

# Write scores to Unity Catalog managed table
logger.info(f"Writing to inference table: {INFERENCE_FULL_TABLE}")

query_delta = (
    df_with_scores.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/rt__inference_delta")
    .trigger(processingTime="10 seconds")
    .toTable(INFERENCE_FULL_TABLE)
)

# COMMAND ----------

# Publish to Event Hubs for Fabric dashboard
logger.info(f"Publishing to Event Hub: {EVENTHUB_NAMESPACE}/{EVENTHUB_NAME}")

ehConf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        f"{EVENTHUB_CONNECTION_STRING};EntityPath={EVENTHUB_NAME}"
    ),
}

df_to_eventhub = (
    df_with_scores
    .select(
        to_json(struct(
            col("contract_id"),
            col("vector_time"),
            col("model_id"),
            col("model_version"),
            col("close_price"),
            col("prediction"),
            col("prob_0"),
            col("prob_1"),
            col("inference_error"),
            col("inference_time"),
        )).alias("body")
    )
)

query_eventhub = (
    df_to_eventhub.writeStream
    .format("eventhubs")
    .options(**ehConf)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/rt__inference_eventhub")
    .trigger(processingTime="10 seconds")
    .start()
)

# COMMAND ----------

# Monitor both streams
logger.info("Streaming queries started. Monitoring progress...")

def log_progress():
    if query_delta.isActive:
        progress = query_delta.lastProgress
        if progress:
            logger.info(f"Delta query progress: {json.dumps(progress, indent=2)}")
    
    if query_eventhub.isActive:
        progress = query_eventhub.lastProgress
        if progress:
            logger.info(f"EventHub query progress: {json.dumps(progress, indent=2)}")

# Wait for both queries
spark.streams.awaitAnyTermination()
