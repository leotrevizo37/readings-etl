from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper

from ..resources import SparkResource


def datalake_readings_extended(
        spark_res: SparkResource,
        raw_dimensions: DataFrame,
        fetch_readings_from: datetime,
        fetch_readings_to: datetime) -> Optional[DataFrame]:
    datalake_readings_df = (
        spark_res.read_lecturas_local_range(fetch_readings_from, fetch_readings_to)
        .select("ReadId", "TimeSpan", "LocalTimeSpan", "Value", "SensorId")
        .dropDuplicates(["ReadId"])
    )

    readings_extended_df = (
        datalake_readings_df
        .alias("rd")
        .join(
            raw_dimensions.alias("dim"),
            upper(col("rd.SensorId").cast("string")) == upper(col("dim.SensorId").cast("string")),
            how="left"
        )
        .select(
            col("rd.ReadId"),
            col("rd.TimeSpan"),
            col("rd.LocalTimeSpan"),
            col("rd.Value"),
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

    if readings_extended_df.rdd.isEmpty():
        return None

    return readings_extended_df
