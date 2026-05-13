from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit, expr

from .lecturas_time_hours import lecturas_time_hours
from .stats_calculator import stats_calculator


def luces(
    factlecturas_extended_df: DataFrame,
    dimensions_df: DataFrame,
    min_hour: str,
    max_hour: str) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter(col("device_type") == "Luminaria")
    )

    if base.rdd.isEmpty():
        empty_base_df = (
            lecturas_time_hours(
                dimensions_df.filter(
                    (
                        (col("device_type") == "Luminaria") &
                        (col("sensor_type") == "Encendido")
                    )
                ),
                min_hour,
                max_hour
            )
        )
        group_with_guid_df = (
            empty_base_df
            .withColumn(
                "OperationId",
                expr("lower(concat_ws('-', substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 1, 8), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 9, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 13, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 17, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 21, 12)))")
            )
            .withColumn("ReadingsCount", lit(0))
            .withColumn("Anomalies", lit(1).cast("integer"))
            .withColumn("MeasurementValue", lit(None))
            .withColumn("MeasurementStdDev", lit(None))
        )

        return group_with_guid_df

    states_stats_df = (
        stats_calculator(
            base,
            "Encendido",
            None,
            None
        )
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "SensorId",
            "LocalTimeSpan",
            "MeasurementValue",
            "MeasurementStdDev",
            "Anomalies",
            "sensor_type",
            "ReadingsCount",
            "device_type",
        )
    )

    luces_df = (
        states_stats_df
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "SensorId",
            "LocalTimeSpan",
            "MeasurementValue",
            "MeasurementStdDev",
            "Anomalies",
            "sensor_type",
            "ReadingsCount",
            "device_type",
        )
    )

    scaffold_df = (
        lecturas_time_hours(
            dimensions_df.filter(
                (col("device_type") == "Luminaria") &
                (col("sensor_type") == "Encendido")
            ),
            min_hour,
            max_hour
        )
        .join(
            luces_df,
            on=[
                "SensorId",
                "DeviceId",
                "LocalTimeSpan",
                "SensorTyId",
                "device_type",
                "LocationId",
                "sensor_type"
            ],
            how="left"
        )
    )

    scaffold_enriched = (
        scaffold_df
        .fillna({"ReadingsCount": 0, "Anomalies": -1})
    )

    if scaffold_enriched.rdd.isEmpty():
        return None

    group_with_guid_df = (
        scaffold_enriched
        .withColumn(
            "OperationId",
            expr("lower(concat_ws('-', substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 1, 8), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 9, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 13, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 17, 4), substring(md5(concat(SensorId, cast(LocalTimeSpan as string))), 21, 12)))")
        )
    )

    return group_with_guid_df
