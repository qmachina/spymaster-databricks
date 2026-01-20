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
    dbutils.widgets.text("ml_container", "ml-artifacts")
    dbutils.widgets.text("underlier", "ES")
    dbutils.widgets.text("instrument_type", "FUT")
    dbutils.widgets.text("run_date", "")

    date = read_widget(dbutils, "date")
    storage_account = read_widget(dbutils, "storage_account")
    lake_container = read_widget(dbutils, "lake_container")
    ml_container = read_widget(dbutils, "ml_container")
    underlier = read_widget(dbutils, "underlier")
    instrument_type = read_widget(dbutils, "instrument_type")
    run_date = read_widget(dbutils, "run_date")

    model_id = f"{underlier}_MODEL"

    training_path = (
        f"abfss://{lake_container}@{storage_account}.dfs.core.windows.net/"
        "gold/training_view"
    )
    df = (
        spark.read.format("delta")
        .load(training_path)
        .where(
            (F.col("session_date") == date)
            & (F.col("underlier") == underlier)
            & (F.col("instrument_type") == instrument_type)
        )
    )

    if df.rdd.isEmpty():
        raise ValueError("No training rows found for the requested partition")

    df_out = df.withColumn("model_id", F.lit(model_id))

    snapshot_path = (
        f"abfss://{ml_container}@{storage_account}.dfs.core.windows.net/"
        f"datasets/{model_id}/{run_date}"
    )
    df_out.write.mode("overwrite").parquet(snapshot_path)


main()
