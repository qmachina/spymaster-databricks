# Databricks notebook source
# MAGIC %md
# MAGIC # smoke__end_to_end
# MAGIC Minimal end-to-end smoke pipeline: Kafka ETL -> train -> score.

# COMMAND ----------

import json
import logging
import time

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, from_json, current_timestamp, avg, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, IntegerType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("smoke_end_to_end")

# COMMAND ----------

dbutils.widgets.text("eventhub_namespace", "ehnspymasterdevoxxrlojskvxey")
dbutils.widgets.text("eventhub_name", "mbo_raw")
dbutils.widgets.text("consumer_group", "smoke_pipeline")
dbutils.widgets.text("checkpoint_base", "abfss://lake@spymasterdevlakeoxxrlojs.dfs.core.windows.net/checkpoints")
dbutils.widgets.text("catalog_bronze", "bronze")
dbutils.widgets.text("schema_bronze", "default")
dbutils.widgets.text("table_bronze", "mbo_smoke")
dbutils.widgets.text("catalog_gold", "gold")
dbutils.widgets.text("schema_gold", "default")
dbutils.widgets.text("table_model_registry", "mbo_smoke_models")
dbutils.widgets.text("table_scores", "mbo_smoke_scores")
dbutils.widgets.text("starting_offsets", "latest")
dbutils.widgets.text("max_offsets_per_trigger", "10000")
dbutils.widgets.text("kafka_request_timeout_ms", "60000")
dbutils.widgets.text("kafka_session_timeout_ms", "30000")
dbutils.widgets.text("min_records", "10")

EVENTHUB_NAMESPACE = dbutils.widgets.get("eventhub_namespace")
EVENTHUB_NAME = dbutils.widgets.get("eventhub_name")
CONSUMER_GROUP = dbutils.widgets.get("consumer_group")
CHECKPOINT_BASE = dbutils.widgets.get("checkpoint_base")
CATALOG_BRONZE = dbutils.widgets.get("catalog_bronze")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
TABLE_BRONZE = dbutils.widgets.get("table_bronze")
CATALOG_GOLD = dbutils.widgets.get("catalog_gold")
SCHEMA_GOLD = dbutils.widgets.get("schema_gold")
TABLE_MODEL_REGISTRY = dbutils.widgets.get("table_model_registry")
TABLE_SCORES = dbutils.widgets.get("table_scores")
STARTING_OFFSETS = dbutils.widgets.get("starting_offsets")
MAX_OFFSETS_PER_TRIGGER = dbutils.widgets.get("max_offsets_per_trigger")
KAFKA_REQUEST_TIMEOUT_MS = dbutils.widgets.get("kafka_request_timeout_ms")
KAFKA_SESSION_TIMEOUT_MS = dbutils.widgets.get("kafka_session_timeout_ms")
MIN_RECORDS = int(dbutils.widgets.get("min_records"))

EVENTHUB_CONNECTION_STRING = dbutils.secrets.get(scope="spymaster", key="eventhub-connection-string")
if not EVENTHUB_CONNECTION_STRING:
    raise ValueError("eventhub-connection-string is required")
if not EVENTHUB_NAMESPACE:
    raise ValueError("eventhub_namespace is required")
if not EVENTHUB_NAME:
    raise ValueError("eventhub_name is required")

eventhub_conn_str = EVENTHUB_CONNECTION_STRING.split(";EntityPath=")[0].strip()
if not eventhub_conn_str:
    raise ValueError("eventhub-connection-string is empty")

bronze_table = f"{CATALOG_BRONZE}.{SCHEMA_BRONZE}.{TABLE_BRONZE}"
model_registry_table = f"{CATALOG_GOLD}.{SCHEMA_GOLD}.{TABLE_MODEL_REGISTRY}"
scores_table = f"{CATALOG_GOLD}.{SCHEMA_GOLD}.{TABLE_SCORES}"

logger.info("Smoke pipeline config loaded")

# COMMAND ----------

event_schema = StructType([
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

kafka_options = {
    "kafka.bootstrap.servers": f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EVENTHUB_NAME,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="$ConnectionString" password="{eventhub_conn_str}";'
    ),
    "startingOffsets": STARTING_OFFSETS,
    "kafka.group.id": CONSUMER_GROUP,
    "maxOffsetsPerTrigger": MAX_OFFSETS_PER_TRIGGER,
    "kafka.request.timeout.ms": KAFKA_REQUEST_TIMEOUT_MS,
    "kafka.session.timeout.ms": KAFKA_SESSION_TIMEOUT_MS,
}

logger.info("Starting Kafka ETL step")

df_raw = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

df_parsed = (
    df_raw
    .select(
        from_json(col("value").cast("string"), event_schema).alias("data"),
        col("timestamp").alias("kafka_time")
    )
    .filter(col("data").isNotNull())
    .select("data.*", "kafka_time")
    .withColumn("ingest_ts", current_timestamp())
)

etl_query = (
    df_parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/smoke__eh_to_delta")
    .trigger(once=True)
    .toTable(bronze_table)
)

etl_query.awaitTermination()
logger.info("Kafka ETL step complete")

# COMMAND ----------

df_train = spark.read.table(bronze_table)
record_count = df_train.count()
if record_count < MIN_RECORDS:
    raise ValueError(f"Need at least {MIN_RECORDS} rows in {bronze_table}, found {record_count}")

row_window = Window.orderBy(col("event_time"))
df_train = (
    df_train
    .withColumn("row_id", row_number().over(row_window))
    .withColumn("label", (col("row_id") % 2).cast("int"))
    .withColumn("price", col("price").cast("double"))
    .withColumn("size", col("size").cast("double"))
)

label_count = df_train.select("label").distinct().count()
if label_count < 2:
    raise ValueError("Need at least two label classes to train")

assembler = VectorAssembler(inputCols=["price", "size"], outputCol="features")
classifier = LogisticRegression(featuresCol="features", labelCol="label")
pipeline = Pipeline(stages=[assembler, classifier])

logger.info("Training model")
model = pipeline.fit(df_train)

model_path = f"dbfs:/tmp/spymaster/smoke_model"
model.write().overwrite().save(model_path)

preds = model.transform(df_train)
accuracy = (
    preds
    .select((col("prediction") == col("label")).cast("int").alias("correct"))
    .agg(avg("correct").alias("accuracy"))
    .collect()[0]["accuracy"]
)

metrics_df = spark.createDataFrame(
    [{
        "model_path": model_path,
        "trained_at": time.time_ns(),
        "record_count": int(record_count),
        "accuracy": float(accuracy),
    }]
)

metrics_df.write.mode("append").saveAsTable(model_registry_table)
logger.info("Training step complete")

# COMMAND ----------

logger.info("Scoring with trained model")
loaded_model = PipelineModel.load(model_path)
scored = (
    loaded_model.transform(df_train)
    .select(
        "contract_id",
        "event_time",
        "price",
        "size",
        col("prediction").cast("int").alias("prediction"),
        col("probability").alias("probability"),
    )
    .withColumn("scored_at", current_timestamp())
)

scored.write.mode("append").saveAsTable(scores_table)
logger.info("Scoring step complete")
