from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession, functions as F, Window


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

    silver_features_path = (
        f"abfss://{lake_container}@{storage_account}.dfs.core.windows.net/"
        "silver/feature_primitives"
    )
    df = (
        spark.read.format("delta")
        .load(silver_features_path)
        .where(
            (F.col("session_date") == date)
            & (F.col("underlier") == underlier)
            & (F.col("instrument_type") == instrument_type)
        )
    )

    if df.rdd.isEmpty():
        raise ValueError("No silver rows found for the requested partition")

    w = Window.partitionBy("contract_id").orderBy("bucket_ns")
    df_vec = (
        df.withColumn("mid_change", F.col("mid") - F.lag("mid").over(w))
        .withColumn(
            "mid_future",
            F.lead("mid", 24).over(w),
        )
        .withColumn(
            "label",
            F.when(F.col("mid_future").isNull(), F.lit(None)).when(
                F.col("mid_future") > F.col("mid"), F.lit(1)
            ).otherwise(F.lit(0)),
        )
    )

    df_vec = df_vec.withColumn(
        "feature_vector",
        F.array(
            F.coalesce(F.col("ofi_ratio"), F.lit(0.0)),
            F.coalesce(F.col("spread"), F.lit(0.0)),
            F.coalesce(F.col("mid_change"), F.lit(0.0)),
            F.coalesce(F.col("vwap"), F.lit(0.0)),
            F.coalesce(F.col("event_count").cast("double"), F.lit(0.0)),
        ),
    )

    df_vec = df_vec.withColumn(
        "feature_hash",
        F.sha2(F.to_json(F.col("feature_vector")), 256),
    )

    df_vec = (
        df_vec.withColumn("vector_time", F.col("bucket_ts"))
        .withColumn("model_id", F.concat(F.col("underlier"), F.lit("_MODEL")))
        .withColumn("lookback_spec", F.lit("5s"))
        .withColumn("data_version", F.lit("silver.feature_primitives"))
    )

    df_setup = df_vec.select(
        "vector_time",
        "model_id",
        "contract_id",
        "lookback_spec",
        "feature_vector",
        "feature_hash",
        "label",
        "data_version",
        "session_date",
        "underlier",
        "instrument_type",
    )

    replace_where = (
        f"session_date = '{date}' and underlier = '{underlier}' "
        f"and instrument_type = '{instrument_type}'"
    )

    gold_setup_path = (
        f"abfss://{lake_container}@{storage_account}.dfs.core.windows.net/"
        "gold/setup_vectors"
    )
    (
        df_setup.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .partitionBy("session_date", "underlier", "instrument_type")
        .save(gold_setup_path)
    )

    df_labels = df_setup.select(
        "vector_time",
        "model_id",
        "contract_id",
        "feature_hash",
        "label",
        "session_date",
        "underlier",
        "instrument_type",
    )

    gold_labels_path = (
        f"abfss://{lake_container}@{storage_account}.dfs.core.windows.net/"
        "gold/labels"
    )
    (
        df_labels.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .partitionBy("session_date", "underlier", "instrument_type")
        .save(gold_labels_path)
    )

    gold_training_path = (
        f"abfss://{lake_container}@{storage_account}.dfs.core.windows.net/"
        "gold/training_view"
    )
    (
        df_setup.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .partitionBy("session_date", "underlier", "instrument_type")
        .save(gold_training_path)
    )


main()
