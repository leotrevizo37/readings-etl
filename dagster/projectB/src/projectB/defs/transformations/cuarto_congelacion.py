from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, lit, col, from_utc_timestamp
from pyspark import StorageLevel

from .lecturas_time_hours import lecturas_time_hours
from .stats_calculator import stats_calculator


DEVICE_TYPE_PATTERN = "(?iu)^(?:cuarto\\s+congelaci(?:o|\\u00f3)n|freezer\\s+room|(?:c[aâ]mara|sala)\\s+de\\s+congelamento)$"
DEVICE_NAME_PATTERN = "(?iu)^(?:cuarto\\s+de\\s+congelaci(?:o|\\u00f3)n|freezer\\s+room|(?:c[aâ]mara|sala)\\s+de\\s+congelamento)$"
SENSOR_TYPE_PATTERN = "(?iu)^(temperatura|temperature|humedad|humidity|umidade|medici(?:o|\\u00f3)n\\s+de\\s+corriente|current\\s+measurement|medi[cç][aã]o\\s+de\\s+corrente)$"
SENSOR_NAME_PATTERN = "(?iu)^(temperatura|temperature|humedad|humidity|umidade|(?:fase|phase)\\s+[123])$"


def cuarto_congelacion(
    factlecturas_extended_df: DataFrame,
    dimensions_df: DataFrame,
    min_hour: str,
    max_hour: str,
) -> Optional[DataFrame]:
    base = factlecturas_extended_df.filter(
        col("device_type").rlike(DEVICE_TYPE_PATTERN)
    )
    scaffold_base_df = lecturas_time_hours(
        dimensions_df.filter(
            col("device_type").rlike(DEVICE_TYPE_PATTERN)
            & col("sensor_type").rlike(SENSOR_TYPE_PATTERN)
        ),
        min_hour,
        max_hour,
    )

    if scaffold_base_df is None:
        return None

    if len(base.head(1)) == 0:
        group_with_guid_df = (
            scaffold_base_df
            .withColumn(
                "LocalTimeSpan",
                from_utc_timestamp(col("TimeSpan"), col("TimeZone"))
            )
            .withColumn(
                "OperationId",
                expr("lower(concat_ws('-', substring(md5(concat(SensorId, cast(TimeSpan as string))), 1, 8), substring(md5(concat(SensorId, cast(TimeSpan as string))), 9, 4), substring(md5(concat(SensorId, cast(TimeSpan as string))), 13, 4), substring(md5(concat(SensorId, cast(TimeSpan as string))), 17, 4), substring(md5(concat(SensorId, cast(TimeSpan as string))), 21, 12)))")
            )
            .withColumn("ReadingsCount", lit(0))
            .withColumn("Min", lit(None))
            .withColumn("Max", lit(None))
            .withColumn("Anomalies", lit(0).cast("integer"))
            .withColumn("HasAnomalies", lit(False))
            .withColumn("CorrectReadingsJson", lit("[]"))
            .withColumn("AnomalousReadingsJson", lit("[]"))
            .withColumn("MeasurementValue", lit(None))
            .withColumn("MeasurementStdDev", lit(None))
        )

        return group_with_guid_df

    base = base.persist(StorageLevel.MEMORY_AND_DISK)

    temperature_stats_df = stats_calculator(
        base,
        "Temperatura",
        -30,
        0
    )
    if temperature_stats_df is not None:
        temperature_stats_df = (
            temperature_stats_df
            .select(
                "DeviceId",
                "LocationId",
                "SensorTyId",
                "SensorId",
                "TimeSpan",
                "MeasurementValue",
                "MeasurementStdDev",
                "Min",
                "Max",
                "Anomalies",
                "HasAnomalies",
                "CorrectReadingsJson",
                "AnomalousReadingsJson",
                "sensor_type",
                "ReadingsCount",
                "device_type",
            )
        )

    humidity_stats_df = stats_calculator(
        base,
        "Humedad",
        25,
        99
    )
    if humidity_stats_df is not None:
        humidity_stats_df = (
            humidity_stats_df
            .select(
                "DeviceId",
                "LocationId",
                "SensorTyId",
                "SensorId",
                "TimeSpan",
                "MeasurementValue",
                "MeasurementStdDev",
                "Min",
                "Max",
                "Anomalies",
                "HasAnomalies",
                "CorrectReadingsJson",
                "AnomalousReadingsJson",
                "sensor_type",
                "ReadingsCount",
                "device_type",
            )
        )

    current_stats_df = stats_calculator(
        base,
        "Medicion de corriente",
        0,
        20
    )
    if current_stats_df is not None:
        current_stats_df = (
            current_stats_df
            .select(
                "DeviceId",
                "LocationId",
                "SensorTyId",
                "SensorId",
                "TimeSpan",
                "MeasurementValue",
                "MeasurementStdDev",
                "Min",
                "Max",
                "Anomalies",
                "HasAnomalies",
                "CorrectReadingsJson",
                "AnomalousReadingsJson",
                "sensor_type",
                "ReadingsCount",
                "device_type",
            )
        )

    stats_dfs = [
        stats_df
        for stats_df in (temperature_stats_df, humidity_stats_df, current_stats_df)
        if stats_df is not None
    ]

    if not stats_dfs:
        base.unpersist()
        return None

    cuarto_congelacion_df = stats_dfs[0]
    for stats_df in stats_dfs[1:]:
        cuarto_congelacion_df = cuarto_congelacion_df.unionByName(stats_df)

    cuarto_congelacion_df = (
        cuarto_congelacion_df
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "SensorId",
            "TimeSpan",
            "MeasurementValue",
                "MeasurementStdDev",
                "Min",
                "Max",
                "Anomalies",
                "HasAnomalies",
                "CorrectReadingsJson",
                "AnomalousReadingsJson",
            "sensor_type",
            "ReadingsCount",
            "device_type",
        )
    )

    base.unpersist()

    scaffold_df = (
        scaffold_base_df
        .join(
            cuarto_congelacion_df,
            on=[
                "SensorId",
                "DeviceId",
                "TimeSpan",
                "SensorTyId",
                "device_type",
                "LocationId",
                "sensor_type"
            ],
            how="left",
        )
    )

    scaffold_enriched = scaffold_df.fillna({
        "ReadingsCount": 0,
        "Anomalies": 0,
        "HasAnomalies": False,
        "CorrectReadingsJson": "[]",
        "AnomalousReadingsJson": "[]",
    })

    group_with_guid_df = (
        scaffold_enriched
        .withColumn(
            "LocalTimeSpan",
            from_utc_timestamp(col("TimeSpan"), col("TimeZone"))
        )
        .withColumn(
            "OperationId",
            expr("lower(concat_ws('-', substring(md5(concat(SensorId, cast(TimeSpan as string))), 1, 8), substring(md5(concat(SensorId, cast(TimeSpan as string))), 9, 4), substring(md5(concat(SensorId, cast(TimeSpan as string))), 13, 4), substring(md5(concat(SensorId, cast(TimeSpan as string))), 17, 4), substring(md5(concat(SensorId, cast(TimeSpan as string))), 21, 12)))")
        )
    )

    return group_with_guid_df
