import json
from pathlib import Path

import databento as db
import pandas as pd
from databento.common.enums import PriceType
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession


def window_ns(session_date: str, start_time: str, end_time: str) -> tuple[int, int]:
    start_local = pd.Timestamp(f"{session_date} {start_time}:00", tz="America/New_York")
    end_local = pd.Timestamp(f"{session_date} {end_time}:00", tz="America/New_York")
    start_ns = int(start_local.tz_convert("UTC").value)
    end_ns = int(end_local.tz_convert("UTC").value)
    if end_ns <= start_ns:
        raise ValueError("End time must be after start time")
    return start_ns, end_ns


def read_widget(dbutils: DBUtils, name: str) -> str:
    value = dbutils.widgets.get(name)
    if not value:
        raise ValueError(f"Missing widget value: {name}")
    return value


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    dbutils.widgets.text("date", "")
    dbutils.widgets.text("start_time", "09:30")
    dbutils.widgets.text("end_time", "11:30")
    dbutils.widgets.text("storage_account", "")
    dbutils.widgets.text("raw_container", "raw-dbn")
    dbutils.widgets.text("lake_container", "lake")
    dbutils.widgets.text("input_blob", "")
    dbutils.widgets.text("underlier", "ES")
    dbutils.widgets.text("instrument_type", "FUT")

    date = read_widget(dbutils, "date")
    start_time = read_widget(dbutils, "start_time")
    end_time = read_widget(dbutils, "end_time")
    storage_account = read_widget(dbutils, "storage_account")
    raw_container = read_widget(dbutils, "raw_container")
    lake_container = read_widget(dbutils, "lake_container")
    input_blob = read_widget(dbutils, "input_blob")
    underlier = read_widget(dbutils, "underlier")
    instrument_type = read_widget(dbutils, "instrument_type")

    raw_path = (
        f"abfss://{raw_container}@{storage_account}.dfs.core.windows.net/"
        f"{input_blob}"
    )
    local_path = Path("/tmp") / Path(input_blob).name
    dbutils.fs.cp(raw_path, f"file:{local_path}", True)

    store = db.DBNStore.from_file(str(local_path))
    df = store.to_df(price_type=PriceType.FIXED, pretty_ts=False, map_symbols=True).reset_index()
    if df.empty:
        raise ValueError("DBN file returned no rows")

    df = df.loc[df["rtype"] == 160].copy()
    df = df.loc[df["symbol"].notna()].copy()
    df = df.loc[~df["symbol"].astype(str).str.contains("-", regex=False)].copy()
    if df.empty:
        raise ValueError("DBN file has no outright MBO rows")

    start_ns, end_ns = window_ns(date, start_time, end_time)
    df = df.loc[(df["ts_event"] >= start_ns) & (df["ts_event"] < end_ns)].copy()
    if df.empty:
        raise ValueError("No rows in the requested time window")

    payload_fields = [
        "instrument_id",
        "publisher_id",
        "flags",
        "channel_id",
        "ts_in_delta",
        "rtype",
    ]
    missing_payload = [f for f in payload_fields if f not in df.columns]
    if missing_payload:
        raise ValueError(f"Missing payload fields: {missing_payload}")

    payload_records = df[payload_fields].to_dict(orient="records")
    payload_json = [json.dumps(rec, separators=(",", ":")) for rec in payload_records]

    df_out = pd.DataFrame(
        {
            "event_time": df["ts_event"].astype("int64"),
            "ingest_time": df["ts_recv"].astype("int64"),
            "venue": str(store.metadata.dataset),
            "symbol": df["symbol"].astype(str),
            "instrument_type": instrument_type,
            "underlier": underlier,
            "contract_id": df["symbol"].astype(str),
            "action": df["action"].astype(str),
            "order_id": df["order_id"].astype("int64"),
            "side": df["side"].astype(str),
            "price": df["price"].astype("float64"),
            "size": df["size"].astype("int64"),
            "sequence": df["sequence"].astype("int64"),
            "payload": payload_json,
            "session_date": date,
        }
    )

    out_path = (
        f"abfss://{lake_container}@{storage_account}.dfs.core.windows.net/"
        "bronze/mbo_envelope"
    )
    replace_where = (
        f"session_date = '{date}' and underlier = '{underlier}' "
        f"and instrument_type = '{instrument_type}'"
    )

    spark_df = spark.createDataFrame(df_out)
    (
        spark_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .partitionBy("session_date", "underlier", "instrument_type")
        .save(out_path)
    )


main()
