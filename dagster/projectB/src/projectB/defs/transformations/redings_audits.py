from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    count_distinct,
    date_trunc,
    explode,
    expr,
    floor,
    from_utc_timestamp,
    greatest,
    lag,
    least,
    lit,
    max as spark_max,
    min as spark_min,
    sequence,
    to_timestamp,
    trim,
    unix_timestamp,
    upper,
    when,
)


LATE_READINGS_THRESHOLD_MINUTES = 15
CONNECTION_LOST_THRESHOLD_MINUTES = 60
READING_BLOCK_MINUTES = 5


def redings_audits(
    readings_df: DataFrame,
    dimensions_df: DataFrame,
    previous_health_df: DataFrame,
    from_utc: datetime,
    to_utc: datetime,
    as_of_utc: datetime,
) -> Optional[DataFrame]:
    sensors_df = (
        dimensions_df
        .filter(col("Active"))
        .select(
            col("SensorId").cast("string").alias("SensorId"),
            trim(col("TimeZone").cast("string")).alias("TimeZone"),
        )
        .filter(col("SensorId").isNotNull())
        .distinct()
        .withColumn("_SensorIdKey", upper(col("SensorId")))
    )
    if len(
        sensors_df
        .filter(col("TimeZone").isNull() | (col("TimeZone") == ""))
        .head(1)
    ) > 0:
        raise ValueError("Active sensors must have a non-empty TimeZone")
    if len(sensors_df.head(1)) == 0:
        return None

    hours_df = (
        sensors_df
        .limit(1)
        .select(
            explode(
                sequence(
                    to_timestamp(lit(from_utc)),
                    to_timestamp(lit(to_utc)),
                    expr("INTERVAL 1 HOUR"),
                )
            ).alias("TimeSpan")
        )
    )
    scaffold_df = sensors_df.crossJoin(hours_df)

    readings_window = Window.partitionBy("_SensorIdKey").orderBy(
        "_ReadingAt",
        "ReadId",
    )
    readings_base_df = (
        readings_df
        .select(
            col("ReadId").cast("string").alias("ReadId"),
            upper(col("SensorId").cast("string")).alias("_SensorIdKey"),
            col("TimeSpan").cast("timestamp").alias("_ReadingAt"),
        )
        .filter(
            col("ReadId").isNotNull()
            & col("_SensorIdKey").isNotNull()
            & col("_ReadingAt").isNotNull()
        )
        .dropDuplicates(["ReadId"])
        .withColumn(
            "_PreviousTimeSpan",
            lag("_ReadingAt").over(readings_window),
        )
        .withColumn(
            "_GapMinutes",
            (
                unix_timestamp(col("_ReadingAt"))
                - unix_timestamp(col("_PreviousTimeSpan"))
            )
            / lit(60),
        )
        .withColumn("TimeSpan", date_trunc("hour", col("_ReadingAt")))
        .withColumn(
            "_ReadingBlock",
            floor(
                (
                    unix_timestamp(col("_ReadingAt"))
                    - unix_timestamp(col("TimeSpan"))
                )
                / lit(READING_BLOCK_MINUTES * 60)
            ),
        )
    )
    hourly_readings_df = (
        readings_base_df
        .groupBy("_SensorIdKey", "TimeSpan")
        .agg(
            count("*").cast("integer").alias("ReadingsCount"),
            spark_min("_ReadingAt").alias("_HourFirstReadingAt"),
            spark_max("_ReadingAt").alias("_HourLastReadingAt"),
            avg("_GapMinutes").alias("_AverageGapMinutes"),
            count("_GapMinutes").cast("integer").alias("_GapCount"),
            spark_max("_GapMinutes").alias("_MaxGapMinutes"),
            spark_min("_GapMinutes").alias("_MinGapMinutes"),
            count_distinct("_ReadingBlock")
            .cast("integer")
            .alias("CoveredReadingBlocks"),
        )
    )

    previous_base_df = (
        previous_health_df
        .select(
            upper(col("SensorId").cast("string")).alias("_SensorIdKey"),
            col("TimeSpan").cast("timestamp").alias("TimeSpan"),
            col("LastReadingAt").cast("timestamp").alias("LastReadingAt"),
        )
    )
    previous_before_df = (
        previous_base_df
        .filter(col("TimeSpan") < lit(from_utc))
        .groupBy("_SensorIdKey")
        .agg(spark_max("LastReadingAt").alias("_PreviousLastReadingAt"))
    )
    previous_by_hour_df = (
        previous_base_df
        .filter(col("TimeSpan") >= lit(from_utc))
        .select(
            "_SensorIdKey",
            "TimeSpan",
            col("LastReadingAt").alias("_PreviousHourLastReadingAt"),
        )
    )

    sensor_window = (
        Window
        .partitionBy("_SensorIdKey")
        .orderBy("TimeSpan")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    prior_sensor_window = (
        Window
        .partitionBy("_SensorIdKey")
        .orderBy("TimeSpan")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    result_df = (
        scaffold_df
        .join(
            hourly_readings_df,
            ["_SensorIdKey", "TimeSpan"],
            "left",
        )
        .join(
            previous_by_hour_df,
            ["_SensorIdKey", "TimeSpan"],
            "left",
        )
        .join(previous_before_df, "_SensorIdKey", "left")
        .withColumn(
            "_PriorLastReadingAt",
            greatest(
                col("_PreviousLastReadingAt"),
                spark_max("_HourLastReadingAt").over(prior_sensor_window),
            ),
        )
        .withColumn(
            "_BoundaryGapMinutes",
            when(
                col("_GapCount") < col("ReadingsCount"),
                (
                    unix_timestamp(col("_HourFirstReadingAt"))
                    - unix_timestamp(col("_PriorLastReadingAt"))
                )
                / lit(60),
            ),
        )
        .withColumn(
            "AverageGapMinutes",
            when(
                col("_BoundaryGapMinutes").isNotNull(),
                when(
                    col("_GapCount") == 0,
                    col("_BoundaryGapMinutes"),
                ).otherwise(
                    (
                        col("_AverageGapMinutes") * col("_GapCount")
                        + col("_BoundaryGapMinutes")
                    )
                    / (col("_GapCount") + lit(1))
                ),
            ).otherwise(col("_AverageGapMinutes")),
        )
        .withColumn(
            "MaximumGapMinutes",
            greatest(col("_MaxGapMinutes"), col("_BoundaryGapMinutes")),
        )
        .withColumn(
            "MinimumGapMinutes",
            least(col("_MinGapMinutes"), col("_BoundaryGapMinutes")),
        )
        .withColumn(
            "_CandidateLastReadingAt",
            greatest(
                col("_HourLastReadingAt"),
                col("_PreviousHourLastReadingAt"),
                col("_PreviousLastReadingAt"),
            ),
        )
        .withColumn(
            "LastReadingAt",
            spark_max("_CandidateLastReadingAt").over(sensor_window),
        )
        .withColumn(
            "_EvaluationAt",
            least(
                col("TimeSpan") + expr("INTERVAL 1 HOUR"),
                lit(as_of_utc),
            ),
        )
        .withColumn(
            "MinutesWithoutReadings",
            when(
                col("LastReadingAt").isNull(),
                lit(None).cast("integer"),
            ).otherwise(
                greatest(
                    lit(0),
                    floor(
                        (
                            unix_timestamp(col("_EvaluationAt"))
                            - unix_timestamp(col("LastReadingAt"))
                        )
                        / lit(60)
                    ),
                ).cast("integer")
            ),
        )
        .withColumn(
            "HasLateReadings",
            when(
                (
                    col("MinutesWithoutReadings")
                    >= lit(LATE_READINGS_THRESHOLD_MINUTES)
                )
                | (
                    col("MaximumGapMinutes")
                    >= lit(LATE_READINGS_THRESHOLD_MINUTES)
                ),
                lit(True),
            ).otherwise(lit(False)),
        )
        .withColumn(
            "IsConnectionLost",
            when(
                (
                    col("MinutesWithoutReadings")
                    >= lit(CONNECTION_LOST_THRESHOLD_MINUTES)
                ),
                lit(True),
            ).otherwise(lit(False)),
        )
        .withColumn(
            "ConnectionLostAt",
            when(
                col("IsConnectionLost"),
                col("LastReadingAt")
                + expr(
                    "INTERVAL "
                    + str(CONNECTION_LOST_THRESHOLD_MINUTES)
                    + " MINUTES"
                ),
            ).otherwise(lit(None).cast("timestamp")),
        )
        .fillna({"ReadingsCount": 0, "CoveredReadingBlocks": 0})
        .withColumn(
            "ReadingsCoveragePercentage",
            (
                col("CoveredReadingBlocks")
                / lit(60 // READING_BLOCK_MINUTES)
                * lit(100)
            ).cast("decimal(7,4)"),
        )
        .withColumn(
            "LocalTimeSpan",
            from_utc_timestamp(col("TimeSpan"), col("TimeZone")),
        )
        .select(
            "SensorId",
            "TimeSpan",
            "LocalTimeSpan",
            col("ReadingsCount").cast("integer").alias("ReadingsCount"),
            col("AverageGapMinutes")
            .cast("decimal(18,6)")
            .alias("AverageGapMinutes"),
            col("MaximumGapMinutes")
            .cast("decimal(18,6)")
            .alias("MaximumGapMinutes"),
            col("MinimumGapMinutes")
            .cast("decimal(18,6)")
            .alias("MinimumGapMinutes"),
            "ReadingsCoveragePercentage",
            "CoveredReadingBlocks",
            "HasLateReadings",
            "IsConnectionLost",
            "LastReadingAt",
            "ConnectionLostAt",
            "MinutesWithoutReadings",
        )
    )

    return result_df
