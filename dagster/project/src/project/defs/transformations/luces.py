from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit, expr

from .stats_calculator import stats_calculator

def luces(
        readings_extended_df: DataFrame) -> Optional[DataFrame]:
    base = (
        readings_extended_df
        .filter((col("_device_type") == "Luminaria"))
    )
    sates_stats_df = (
        stats_calculator(
            base,
            "Encendido",
            1)
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "StatusCode",
            "LocalTimeSpan",
            "Promedio",
            "_sensor_type",
        )
        .withColumnRenamed("Promedio", "MeasurementValue")
        .withColumn("Anomalies", lit(None))
    )

    luces_df = (
        sates_stats_df
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "LocalTimeSpan",
            "StatusCode",
            "MeasurementValue",
            "Anomalies",
            "_sensor_type",
        )
    )

    if luces_df.rdd.isEmpty():
        return None

    group_ids_df = (
        luces_df
        .select("DeviceId", "LocalTimeSpan")
        .distinct()
        .withColumn("OperationId", expr("uuid()"))
    )

    group_with_guid_df = (
        luces_df
        .join(group_ids_df, on=["DeviceId", "LocalTimeSpan"], how="left")
    )

    return group_with_guid_df