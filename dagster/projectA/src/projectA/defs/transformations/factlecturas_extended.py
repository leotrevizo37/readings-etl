from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp

from ..resources import SqlServerResource, SparkResource


LOCAL_TIME_SPAN_FORMAT = "yyyy-MM-dd HH:mm:ss"


def _build_factlecturas_query(fetch_factlecturas_from: Optional[str]) -> str:
    base_query = (
        "SELECT ReadId, "
        "TimeSpan, "
        "CONVERT(VARCHAR(19), LocalTimeSpan, 120) AS LocalTimeSpanText, "
        "Value, "
        "SensorId "
        "FROM dwh.FactLecturas"
    )
    if (
        fetch_factlecturas_from is None
        or fetch_factlecturas_from == ""
        or fetch_factlecturas_from == "None"
    ):
        return "(" + base_query + ") AS src"

    return (
        "("
        + base_query
        + " WHERE LocalTimeSpan >= '"
        + str(fetch_factlecturas_from)
        + "') AS src"
    )


def _restore_local_timespan(df: DataFrame) -> DataFrame:
    # LocalTimeSpan already comes in local wall-clock time from SQL Server.
    # Reading it as text avoids the JDBC timestamp timezone shift.
    return (
        df.withColumn(
            "LocalTimeSpan",
            to_timestamp(col("LocalTimeSpanText"), LOCAL_TIME_SPAN_FORMAT),
        )
        .drop("LocalTimeSpanText")
    )


def factlecturas_extended(
        spark_res: SparkResource,
        sqlserver: SqlServerResource,
        raw_dimensions: DataFrame,
        fetch_factlecturas_from: str) -> Optional[DataFrame]:
    spark = spark_res.get_spark()
    dwh_factlecturas_df = _restore_local_timespan(
        spark.read.format("jdbc")
        .option("dbtable", _build_factlecturas_query(fetch_factlecturas_from))
        .options(**sqlserver.get_basic_options())
        .load()
    )

    dwh_factlecturas_extended_df = (
        dwh_factlecturas_df
        .alias("fl")
        .join(
            raw_dimensions.alias("dim"), on="SensorId", how="left"
        )
        .select(
            col("fl.ReadId"),
            col("fl.TimeSpan"),
            col("fl.LocalTimeSpan"),
            col("fl.Value"),
            col("dim.SensorId"),
            col("dim.SensorTyId"),
            col("dim.DeviceId"),
            col("dim.sensor_name"),
            col("dim.sensor_type"),
            col("dim.DeviceTyId"),
            col("dim.SubLocationId"),
            col("dim.device_name"),
            col("dim.device_type"),
            col("dim.SubLocationTyId"),
            col("dim.sublocation_name"),
            col("dim.LocationId"),
            col("dim.sublocation_type")
        )
    )

    if dwh_factlecturas_extended_df.rdd.isEmpty():
        return None

    return dwh_factlecturas_extended_df
