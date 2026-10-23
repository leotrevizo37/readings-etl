from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, from_utc_timestamp, lit, trim

from .lecturas_time_hours import lecturas_time_hours
from .stats_calculator import stats_calculator


DEVICE_TYPE_PATTERN = "(?iu)^(acometida|(?:electrical\\s+)?service\\s+entrance|entrada\\s+de\\s+energia)$"
SENSOR_TYPE_PATTERN = "(?iu)^(corriente\\s+electrica|electric\\s+current|electrical\\s+current|corrente\\s+elétrica|corrente\\s+eletrica)$"


def acometida(
    factlecturas_extended_df: DataFrame,
    dimensions_df: DataFrame,
    min_hour: str,
    max_hour: str,
) -> Optional[DataFrame]:
    sensor_types = ("CORRIENTE ELECTRICA",)
    base = factlecturas_extended_df.filter(trim(col("device_type")).rlike(DEVICE_TYPE_PATTERN))
    scaffold_base_df = lecturas_time_hours(
        dimensions_df.filter(
            (trim(col("device_type")).rlike(DEVICE_TYPE_PATTERN))
            & trim(col("sensor_type")).rlike(SENSOR_TYPE_PATTERN)
        ),
        min_hour,
        max_hour,
    )

    if scaffold_base_df is None:
        return None

    stats_dfs = []
    for sensor_type in sensor_types:
        stats_df = stats_calculator(base, sensor_type)
        if stats_df is not None:
            stats_dfs.append(stats_df.drop("device_name"))

    if stats_dfs:
        stats_df = stats_dfs[0]
        for other_stats_df in stats_dfs[1:]:
            stats_df = stats_df.unionByName(other_stats_df)

        scaffold_df = scaffold_base_df.join(
            stats_df,
            on=[
                "SensorId",
                "DeviceId",
                "TimeSpan",
                "SensorTyId",
                "device_type",
                "LocationId",
                "sensor_type",
            ],
            how="left",
        ).fillna({
            "ReadingsCount": 0,
            "Anomalies": 0,
            "HasAnomalies": False,
            "CorrectReadingsJson": "[]",
            "AnomalousReadingsJson": "[]",
        })
    else:
        scaffold_df = (
            scaffold_base_df
            .withColumn("ReadingsCount", lit(0).cast("long"))
            .withColumn("Min", lit(None).cast("double"))
            .withColumn("Max", lit(None).cast("double"))
            .withColumn("Anomalies", lit(0).cast("integer"))
            .withColumn("HasAnomalies", lit(False))
            .withColumn("CorrectReadingsJson", lit("[]"))
            .withColumn("AnomalousReadingsJson", lit("[]"))
            .withColumn("MeasurementValue", lit(None).cast("double"))
            .withColumn("MeasurementStdDev", lit(None).cast("double"))
        )

    return (
        scaffold_df
        .withColumn(
            "LocalTimeSpan",
            from_utc_timestamp(col("TimeSpan"), col("TimeZone")),
        )
        .withColumn(
            "OperationId",
            expr("lower(concat_ws('-', substring(md5(concat(SensorId, cast(TimeSpan as string))), 1, 8), substring(md5(concat(SensorId, cast(TimeSpan as string))), 9, 4), substring(md5(concat(SensorId, cast(TimeSpan as string))), 13, 4), substring(md5(concat(SensorId, cast(TimeSpan as string))), 17, 4), substring(md5(concat(SensorId, cast(TimeSpan as string))), 21, 12)))"),
        )
    )

