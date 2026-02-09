from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, expr

from .stats_calculator import stats_calculator
from .stats_calculator_time_window import stats_calculator_time_window

def cuartos_refrigeracion(
        readings_extended_df: DataFrame) -> Optional[DataFrame]:
    base = (
        readings_extended_df
        .filter(
            col("_device_type").isin([
                "Cuarto de conservación",
                "Ambiente conservacion",
                "Compresor conservación"
            ])
        )
    )

    temperature_stats_df = (
        stats_calculator_time_window(
            base,
            "Temperatura",
            34,
            40,
            240
        )
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

    current_stats_df = (
        stats_calculator(
            base,
            "Corriente",
            1)
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "LocalTimeSpan",
            "StatusCode",
            "MeasurementValue",
            "_sensor_type",
        )
        .withColumn("Anomalies", lit(None))
    )

    cuartos_refrigeracion_df = (
        temperature_stats_df.unionByName(current_stats_df)
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

    if cuartos_refrigeracion_df.rdd.isEmpty():
        return None

    group_ids_df = (
        cuartos_refrigeracion_df
        .select("DeviceId", "LocalTimeSpan")
        .distinct()
        .withColumn("OperationId", expr("uuid()"))
    )

    group_with_guid_df = (
        cuartos_refrigeracion_df
        .join(group_ids_df, on=["DeviceId", "LocalTimeSpan"], how="left")
    )

    return group_with_guid_df