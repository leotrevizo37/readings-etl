from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, expr

from .stats_calculator import stats_calculator
from .stats_calculator_time_window import stats_calculator_time_window

def cuartos_congelacion(
        readings_extended_df: DataFrame) -> Optional[DataFrame]:
    base = (
        readings_extended_df
        .filter(
            col("_device_type").isin([
                "Cuarto de congelación",
                "Ambiente congelacion",
                "Compresor congelación",
                "Resitencia de deshielo",
                "Resistencia de deshielo"
            ])
        )
    )

    temperature_stats_df = (
        stats_calculator_time_window(
            base,
            "Temperatura",
            -10,
            0,
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
            base
            .filter(
                (col("_sensor_name") != "Resistencia de deshielo") &
                (col("_device_type") != "Resitencia de deshielo") &
                (col("_device_type") != "Resistencia de deshielo")
            ),
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

    cuartos_conservacion_df = (
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

    if cuartos_conservacion_df.rdd.isEmpty():
        return None

    group_ids_df = (
        cuartos_conservacion_df
        .select("DeviceId", "LocalTimeSpan")
        .distinct()
        .withColumn("OperationId", expr("uuid()"))
    )

    group_with_guid_df = (
        cuartos_conservacion_df
        .join(group_ids_df, on=["DeviceId", "LocalTimeSpan"], how="left")
    )

    return group_with_guid_df