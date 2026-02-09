from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit, expr

from .stats_calculator import stats_calculator

def termostatos(
        readings_extended_df: DataFrame) -> Optional[DataFrame]:
    base = (
        readings_extended_df
        .filter(
            (
                    (col("_device_type") == "A/C de Cocina") |
                    (col("_device_type") == "A/C de Comedor") |
                    (col("_device_type") == "Ambiente playland")
            )
        )
    )

    state_stats_df = (
        stats_calculator(
            base,
            "On/Off Termostato",
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

    temperature_stats_df = (
        stats_calculator(
            base,
            "Temperatura",
            66)
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "StatusCode",
            "LocalTimeSpan",
            "MeasurementValue",
            "_sensor_type",
        )
        .withColumn("Anomalies", lit(None))
    )

    termostatos_df = (
        state_stats_df.unionByName(temperature_stats_df)
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

    if termostatos_df.rdd.isEmpty():
        return None

    group_ids_df = (
        termostatos_df
        .select("DeviceId", "LocalTimeSpan")
        .distinct()
        .withColumn("OperationId", expr("uuid()"))
    )

    group_with_guid_df = (
        termostatos_df
        .join(group_ids_df, on=["DeviceId", "LocalTimeSpan"], how="left")
    )

    return group_with_guid_df