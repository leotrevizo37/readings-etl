from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from ..resources import SqlServerResource, SparkResource

def readings_extended(
        spark_res: SparkResource,
        sqlserver: SqlServerResource,
        raw_dimensions: DataFrame,
        fetch_readings_from: str) -> Optional[DataFrame]:
    spark = spark_res.get_spark()

    if (fetch_readings_from is None
            or fetch_readings_from == ""
            or fetch_readings_from == "None"):
        dwh_readings_df = (
            spark.read.format("jdbc")
            .option("dbtable", "dwh.readings")
            .options(**sqlserver.get_basic_options())
            .load()
        )
    else:
        dbtable = (
            "(SELECT * "
            " FROM dwh.readings "
            f" WHERE LocalTimeSpan >= '{fetch_readings_from}') AS src"
        )
        dwh_readings_df =(
            spark.read.format("jdbc")
            .option("dbtable", dbtable)
            .options(**sqlserver.get_basic_options())
            .load()
        )

    dwh_readings_extended_df = (
        dwh_readings_df
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
            col("dim._sensor_name"),
            col("dim._sensor_type"),
            col("dim.DeviceTyId"),
            col("dim.SubLocationId"),
            col("dim._device_name"),
            col("dim._device_type"),
            col("dim.SubLocationTyId"),
            col("dim._sublocation_name"),
            col("dim.LocationId"),
            col("dim._sublocation_type")
        )
    )

    if dwh_readings_extended_df.rdd.isEmpty():
        return None

    return dwh_readings_extended_df
