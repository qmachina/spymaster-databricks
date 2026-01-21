# Databricks notebook source
# MAGIC %md
# MAGIC # rt__bronze_to_silver
# MAGIC Silver streaming job: stateful orderbook reconstruction + rollups.
# MAGIC Uses `applyInPandasWithState` for stateful processing keyed by contract_id.

# COMMAND ----------

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, sum as spark_sum, avg, min as spark_min, max as spark_max,
    first, last, count, lit, current_timestamp, to_date, pandas_udf, from_unixtime
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, IntegerType,
    TimestampType, ArrayType
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
import pandas as pd
import numpy as np
from typing import Iterator, Tuple
from dataclasses import dataclass, asdict
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# Configure Spark for RocksDB state store and optimizations
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
spark.conf.set("spark.databricks.streaming.statefulOperator.asyncCheckpoint.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.databricks.adaptive.autoOptimizeShuffle.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "400")

logger.info("Spark configuration complete")

# COMMAND ----------

# Configuration from widgets
dbutils.widgets.text("catalog_bronze", "bronze")
dbutils.widgets.text("bronze_schema", "default")
dbutils.widgets.text("bronze_table", "mbo_stream")
dbutils.widgets.text("catalog_silver", "silver")
dbutils.widgets.text("silver_schema", "default")
dbutils.widgets.text("silver_table", "feature_primitives")
dbutils.widgets.text("checkpoint_base", "abfss://lake@spymasterdevlakeoxxrlojs.dfs.core.windows.net/checkpoints")
dbutils.widgets.text("max_files_per_trigger", "1000")
dbutils.widgets.text("max_bytes_per_trigger", "1g")

CATALOG_BRONZE = dbutils.widgets.get("catalog_bronze")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
BRONZE_TABLE = dbutils.widgets.get("bronze_table")
CATALOG_SILVER = dbutils.widgets.get("catalog_silver")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
SILVER_TABLE = dbutils.widgets.get("silver_table")
CHECKPOINT_BASE = dbutils.widgets.get("checkpoint_base")
MAX_FILES_PER_TRIGGER = int(dbutils.widgets.get("max_files_per_trigger"))
MAX_BYTES_PER_TRIGGER = dbutils.widgets.get("max_bytes_per_trigger")

BRONZE_FULL_TABLE = f"{CATALOG_BRONZE}.{BRONZE_SCHEMA}.{BRONZE_TABLE}"
SILVER_FULL_TABLE = f"{CATALOG_SILVER}.{SILVER_SCHEMA}.{SILVER_TABLE}"

# COMMAND ----------

# State schema for orderbook
@dataclass
class OrderbookState:
    contract_id: str
    bids: dict  # price -> size
    asks: dict  # price -> size
    last_update_ns: int
    last_trade_price: float
    last_trade_size: int
    total_volume: int

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "OrderbookState":
        return cls(**d)

# COMMAND ----------

# Bar 5s output schema
bar_5s_schema = StructType([
    StructField("contract_id", StringType(), True),
    StructField("bucket_ns", LongType(), True),
    StructField("bucket_ts", TimestampType(), True),
    StructField("session_date", StringType(), True),
    StructField("underlier", StringType(), True),
    StructField("instrument_type", StringType(), True),
    StructField("event_count", LongType(), True),
    StructField("size_total", LongType(), True),
    StructField("buy_size", LongType(), True),
    StructField("sell_size", LongType(), True),
    StructField("vwap", DoubleType(), True),
    StructField("ofi", LongType(), True),
    StructField("ofi_ratio", DoubleType(), True),
    StructField("mid", DoubleType(), True),
    StructField("spread", DoubleType(), True),
    StructField("best_bid", DoubleType(), True),
    StructField("best_ask", DoubleType(), True),
])

# State output schema
state_output_schema = StructType([
    StructField("state_json", StringType(), True),
])

# COMMAND ----------

def process_orderbook_updates(
    key: Tuple[str],
    pdf_iter: Iterator[pd.DataFrame],
    state: GroupState
) -> Iterator[pd.DataFrame]:
    """
    Stateful processing function for orderbook reconstruction.
    Keyed by contract_id, maintains book state across micro-batches.
    Uses vectorized pandas operations instead of row-by-row iteration.
    """
    contract_id = key[0]

    # Load or initialize state
    if state.exists:
        book_state = OrderbookState.from_dict(json.loads(state.get["state_json"]))
    else:
        book_state = OrderbookState(
            contract_id=contract_id,
            bids={},
            asks={},
            last_update_ns=0,
            last_trade_price=0.0,
            last_trade_size=0,
            total_volume=0,
        )

    bars = []
    BAR_DURATION_NS = 5_000_000_000  # 5 seconds

    for pdf in pdf_iter:
        if pdf.empty:
            continue

        try:
            pdf = pdf.sort_values("event_time")
            pdf["action_norm"] = pdf["action"].astype(str).str.upper()
            pdf["side_norm"] = pdf["side"].astype(str).str.upper()

            event_count = int(len(pdf))
            size_total = int(pdf["size"].sum())
            buy_size = int(pdf.loc[pdf["side_norm"].isin(["B", "BUY", "BID"]), "size"].sum())
            sell_size = int(pdf.loc[pdf["side_norm"].isin(["S", "SELL", "ASK", "A"]), "size"].sum())
            notional = float((pdf["price"] * pdf["size"]).sum())
            vwap = notional / size_total if size_total > 0 else 0.0
            ofi = buy_size - sell_size
            ofi_ratio = ofi / (buy_size + sell_size) if (buy_size + sell_size) > 0 else 0.0

            for action_type in pdf["action_norm"].unique():
                action_mask = pdf["action_norm"] == action_type
                
                if action_type == "A":
                    for side_type in pdf[action_mask]["side_norm"].unique():
                        mask = action_mask & (pdf["side_norm"] == side_type)
                        subset = pdf[mask]
                        
                        book = book_state.bids if side_type == "B" else book_state.asks
                        
                        price_agg = subset.groupby("price")["size"].sum()
                        for price, size in price_agg.items():
                            book[price] = book.get(price, 0) + size
                
                elif action_type == "C":
                    for side_type in pdf[action_mask]["side_norm"].unique():
                        mask = action_mask & (pdf["side_norm"] == side_type)
                        subset = pdf[mask]
                        
                        book = book_state.bids if side_type == "B" else book_state.asks
                        
                        price_agg = subset.groupby("price")["size"].sum()
                        for price, size in price_agg.items():
                            if price in book:
                                book[price] = max(0, book[price] - size)
                                if book[price] == 0:
                                    del book[price]
                
                elif action_type == "T":
                    trades = pdf[action_mask]
                    if len(trades) > 0:
                        last_trade = trades.iloc[-1]
                        book_state.last_trade_price = last_trade['price']
                        book_state.last_trade_size = last_trade['size']
                        book_state.total_volume += trades['size'].sum()

            book_state.last_update_ns = int(pdf['event_time'].max())

            best_bid = max(book_state.bids.keys()) if book_state.bids else 0.0
            best_ask = min(book_state.asks.keys()) if book_state.asks else 0.0
            spread = best_ask - best_bid if best_bid and best_ask else 0.0
            mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else 0.0

            bucket_ns = (book_state.last_update_ns // BAR_DURATION_NS) * BAR_DURATION_NS
            bucket_ts = pd.Timestamp(bucket_ns, unit="ns")
            session_date = (
                pd.Timestamp(book_state.last_update_ns, unit="ns", tz="UTC")
                .tz_convert("America/New_York")
                .date()
            )
            underlier = str(pdf["underlier"].iloc[-1])
            instrument_type = str(pdf["instrument_type"].iloc[-1])

            bar = {
                "contract_id": contract_id,
                "bucket_ns": int(bucket_ns),
                "bucket_ts": bucket_ts,
                "session_date": str(session_date),
                "underlier": underlier,
                "instrument_type": instrument_type,
                "event_count": event_count,
                "size_total": size_total,
                "buy_size": buy_size,
                "sell_size": sell_size,
                "vwap": float(vwap),
                "ofi": int(ofi),
                "ofi_ratio": float(ofi_ratio),
                "mid": float(mid_price),
                "spread": float(spread),
                "best_bid": float(best_bid),
                "best_ask": float(best_ask),
            }
            bars.append(bar)
        
        except Exception as e:
            logger.error(f"Error processing contract {contract_id}: {e}")
            continue

    # Update state with EventTimeTimeout for automatic cleanup
    state.update({"state_json": json.dumps(book_state.to_dict())})
    state.setTimeoutDuration(600000)  # 10 minutes timeout

    if bars:
        yield pd.DataFrame(bars)
    else:
        yield pd.DataFrame(columns=bar_5s_schema.fieldNames())

# COMMAND ----------

# Read from Bronze Unity Catalog table with rate limiting
logger.info(f"Reading from bronze table: {BRONZE_FULL_TABLE}")

df_bronze = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
    .option("maxBytesPerTrigger", MAX_BYTES_PER_TRIGGER)
    .table(BRONZE_FULL_TABLE)
)

# Add event time column from event_time field and apply watermark
df_bronze = (
    df_bronze
    .withColumn("event_time_ts", from_unixtime(col("event_time") / lit(1_000_000_000)).cast("timestamp"))
    .withWatermark("event_time_ts", "10 minutes")
)

logger.info("Bronze stream loaded with watermark")

# COMMAND ----------

# Apply stateful processing with EventTimeTimeout for automatic state cleanup
logger.info("Applying stateful orderbook processing...")

df_silver = (
    df_bronze
    .groupBy("contract_id")
    .applyInPandasWithState(
        process_orderbook_updates,
        outputStructType=bar_5s_schema,
        stateStructType=state_output_schema,
        outputMode="append",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
    )
)

logger.info("Stateful processing applied")

# COMMAND ----------

# Write to Silver Unity Catalog managed table
logger.info(f"Writing to silver table: {SILVER_FULL_TABLE}")

query = (
    df_silver.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/rt__silver")
    .trigger(processingTime="10 seconds")
    .toTable(SILVER_FULL_TABLE)
)

# COMMAND ----------

# Monitor stream progress
logger.info("Streaming query started. Monitoring progress...")

def log_progress():
    if query.isActive:
        progress = query.lastProgress
        if progress:
            logger.info(f"Query progress: {json.dumps(progress, indent=2)}")
            
            # Log state metrics
            state_ops = progress.get("stateOperators", [])
            for op in state_ops:
                num_rows = op.get("numRowsInMemory", 0)
                logger.info(f"State rows in memory: {num_rows}")

query.awaitTermination()
