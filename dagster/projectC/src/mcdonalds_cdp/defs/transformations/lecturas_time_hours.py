from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    explode,
    sequence,
    expr,
    lit,
    to_timestamp)


def lecturas_time_hours(
    base: DataFrame,
    min_hour: str,
    max_hour: str
) -> Optional[DataFrame]:

    hours_df = (
        base.select(lit(1).alias("_seed")).limit(1)
        .select(
            explode(
                sequence(
                    to_timestamp(lit(min_hour)),
                    to_timestamp(lit(max_hour)),
                    expr("INTERVAL 1 HOUR")
                )
            ).alias("HourStart"))
    )

    sensors_df = base.select("SensorId", "DeviceId", "SensorTyId", "sensor_type", "device_type", "LocationId").distinct()

    skeleton_df = sensors_df.crossJoin(hours_df)

    current_df = (
        skeleton_df
        .withColumnRenamed("LocalTimeSpan", "_LocalTimeSpan")
        .withColumnRenamed("HourStart", "LocalTimeSpan")
    )

    if current_df.rdd.isEmpty():
        return None

    return current_df
