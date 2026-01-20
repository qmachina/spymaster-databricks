from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession, functions as F


def read_widget(dbutils: DBUtils, name: str) -> str:
    value = dbutils.widgets.get(name)
    if not value:
        raise ValueError(f"Missing widget value: {name}")
    return value


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    dbutils.widgets.text("date", "")
    dbutils.widgets.text("storage_account", "")
    dbutils.widgets.text("lake_container", "lake")
    dbutils.widgets.text("underlier", "ES")
    dbutils.widgets.text("instrument_type", "FUT")

    date = read_widget(dbutils, "date")
    storage_account = read_widget(dbutils, "storage_account")
    lake_container = read_widget(dbutils, "lake_container")
    underlier = read_widget(dbutils, "underlier")
    instrument_type = read_widget(dbutils, "instrument_type")

    bronze_path = (
        f"abfss://{lake_container}@{storage_account}.dfs.core.windows.net/"
        "bronze/mbo_envelope"
    )
    df = (
        spark.read.format("delta")
        .load(bronze_path)
        .where(
            (F.col("session_date") == date)
            & (F.col("underlier") == underlier)
            & (F.col("instrument_type") == instrument_type)
        )
    )

    if df.rdd.isEmpty():
        raise ValueError("No bronze rows found for the requested partition")

    df_norm = (
        df.withColumn(
            "event_ts",
            F.from_unixtime(F.col("event_time") / F.lit(1_000_000_000)).cast("timestamp"),
        )
        .withColumn("event_ts_est", F.from_utc_timestamp(F.col("event_ts"), "America/New_York"))
        .withColumn(
            "side_sign",
            F.when(F.upper(F.col("side")).isin("B", "BUY", "BID"), F.lit(1))
            .when(F.upper(F.col("side")).isin("S", "SELL", "ASK", "A"), F.lit(-1))
            .otherwise(F.lit(0)),
        )
        .withColumn("signed_size", F.col("size") * F.col("side_sign"))
    )

    replace_where = (
        f"session_date = '{date}' and underlier = '{underlier}' "
        f"and instrument_type = '{instrument_type}'"
    )

    silver_norm_path = (
        f"abfss://{lake_container}@{storage_account}.dfs.core.windows.net/"
        "silver/mbo_normalized"
    )
    (
        df_norm.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .partitionBy("session_date", "underlier", "instrument_type")
        .save(silver_norm_path)
    )

    bucket_ns = (
        (F.col("event_time") / F.lit(5_000_000_000)).cast("long") * F.lit(5_000_000_000)
    )
    df_bucket = df_norm.withColumn("bucket_ns", bucket_ns).withColumn(
        "bucket_ts",
        F.from_unixtime(F.col("bucket_ns") / F.lit(1_000_000_000)).cast("timestamp"),
    )

    buy_size = F.sum(F.when(F.col("side_sign") == 1, F.col("size")).otherwise(F.lit(0)))
    sell_size = F.sum(F.when(F.col("side_sign") == -1, F.col("size")).otherwise(F.lit(0)))
    price_last = F.max(F.struct(F.col("event_time"), F.col("price"))).getField("price")

    df_bar = (
        df_bucket.groupBy(
            "session_date",
            "underlier",
            "instrument_type",
            "contract_id",
            "bucket_ns",
            "bucket_ts",
        )
        .agg(
            F.count("*").alias("event_count"),
            F.sum("size").alias("size_total"),
            buy_size.alias("buy_size"),
            sell_size.alias("sell_size"),
            F.sum(F.col("price") * F.col("size")).alias("notional"),
            F.min("price").alias("price_min"),
            F.max("price").alias("price_max"),
            price_last.alias("price_last"),
            F.max(F.when(F.col("side_sign") == 1, F.col("price"))).alias("best_bid"),
            F.min(F.when(F.col("side_sign") == -1, F.col("price"))).alias("best_ask"),
        )
        .withColumn(
            "vwap",
            F.when(F.col("size_total") > 0, F.col("notional") / F.col("size_total")),
        )
        .withColumn("ofi", F.col("buy_size") - F.col("sell_size"))
        .withColumn(
            "ofi_ratio",
            F.when(
                (F.col("buy_size") + F.col("sell_size")) > 0,
                F.col("ofi") / (F.col("buy_size") + F.col("sell_size")),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("spread", F.col("best_ask") - F.col("best_bid"))
        .withColumn("mid", (F.col("best_ask") + F.col("best_bid")) / F.lit(2.0))
    )

    silver_bar_path = (
        f"abfss://{lake_container}@{storage_account}.dfs.core.windows.net/"
        "silver/bar_5s"
    )
    (
        df_bar.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .partitionBy("session_date", "underlier", "instrument_type")
        .save(silver_bar_path)
    )

    df_orderbook = df_bar.select(
        "session_date",
        "underlier",
        "instrument_type",
        "contract_id",
        "bucket_ns",
        "bucket_ts",
        "best_bid",
        "best_ask",
        "mid",
        "spread",
        "buy_size",
        "sell_size",
    )

    silver_orderbook_path = (
        f"abfss://{lake_container}@{storage_account}.dfs.core.windows.net/"
        "silver/orderbook_state"
    )
    (
        df_orderbook.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .partitionBy("session_date", "underlier", "instrument_type")
        .save(silver_orderbook_path)
    )

    df_features = df_bar.select(
        "session_date",
        "underlier",
        "instrument_type",
        "contract_id",
        "bucket_ns",
        "bucket_ts",
        "event_count",
        "size_total",
        "buy_size",
        "sell_size",
        "ofi",
        "ofi_ratio",
        "vwap",
        "mid",
        "spread",
    )

    silver_features_path = (
        f"abfss://{lake_container}@{storage_account}.dfs.core.windows.net/"
        "silver/feature_primitives"
    )
    (
        df_features.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .partitionBy("session_date", "underlier", "instrument_type")
        .save(silver_features_path)
    )


main()
