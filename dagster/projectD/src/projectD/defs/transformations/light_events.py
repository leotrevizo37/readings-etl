from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    collect_set,
    concat,
    concat_ws,
    from_utc_timestamp,
    lag,
    lead,
    lit,
    max,
    md5,
    row_number,
    round as spark_round,
    size,
    substring,
    sum,
    unix_timestamp,
    when,
)
from pyspark.sql.window import Window


LIGHT_SENSOR_PATTERN = r"(?iu)control de luces"
LIGHT_ON_VALUE = 1
LIGHT_OFF_VALUE = 0
COMMUNICATION_THRESHOLD_MINUTES = 60


def light_events(
    factlecturas_extended_df: DataFrame,
    to_utc_str: str,
    previous_completed_events_df: Optional[DataFrame] = None,
) -> DataFrame:
    base = factlecturas_extended_df.filter(
        col("sensor_type").rlike(LIGHT_SENSOR_PATTERN)
    )

    reading_window = Window.partitionBy("SensorId", "TimeSpan")
    base = (
        base
        .withColumn(
            "reading_variants",
            size(collect_set("Value").over(reading_window)),
        )
        .withColumn(
            "reading_row_number",
            row_number().over(reading_window.orderBy("ReadId")),
        )
        .filter(
            (col("reading_variants") == lit(1))
            & (col("reading_row_number") == lit(1))
            & (col("Value").isin(LIGHT_ON_VALUE, LIGHT_OFF_VALUE))
        )
    )

    sensor_window = Window.partitionBy("SensorId").orderBy("TimeSpan")

    with_prev = (
        base
        .withColumn("prev_value", lag("Value").over(sensor_window))
        .withColumn("prev_time", lag("TimeSpan").over(sensor_window))
    )

    with_event_id = (
        with_prev
        .withColumn(
            "is_new_event",
            when(
                col("prev_value").isNull()
                | (col("Value") != col("prev_value"))
                | (
                    unix_timestamp(col("TimeSpan"))
                    - unix_timestamp(col("prev_time"))
                    >= lit(COMMUNICATION_THRESHOLD_MINUTES * 60)
                ),
                lit(1),
            ).otherwise(lit(0)),
        )
        .withColumn(
            "event_id",
            sum("is_new_event").over(
                sensor_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
    )

    event_window = Window.partitionBy("SensorId", "event_id")

    state_changes = (
        with_event_id
        .withColumn("last_event_time", max("TimeSpan").over(event_window))
        .withColumn(
            "event_row_number",
            row_number().over(event_window.orderBy("TimeSpan")),
        )
        .filter(col("event_row_number") == lit(1))
    )

    change_window = Window.partitionBy("SensorId").orderBy("TimeSpan")

    with_next_event = (
        state_changes
        .withColumn("next_event_time", lead("TimeSpan").over(change_window))
    )

    result = with_next_event.withColumn(
        "EventMinutes",
        when(
            col("next_event_time").isNotNull(),
            when(
                unix_timestamp(col("next_event_time"))
                - unix_timestamp(col("last_event_time"))
                >= lit(COMMUNICATION_THRESHOLD_MINUTES * 60),
                (
                    unix_timestamp(col("last_event_time"))
                    - unix_timestamp(col("TimeSpan"))
                )
                / lit(60)
                + lit(COMMUNICATION_THRESHOLD_MINUTES),
            ).otherwise(
                (
                    unix_timestamp(col("next_event_time"))
                    - unix_timestamp(col("TimeSpan"))
                )
                / lit(60)
            ),
        ).otherwise(
            when(
                unix_timestamp(lit(to_utc_str))
                - unix_timestamp(col("last_event_time"))
                >= lit(COMMUNICATION_THRESHOLD_MINUTES * 60),
                (
                    unix_timestamp(col("last_event_time"))
                    - unix_timestamp(col("TimeSpan"))
                )
                / lit(60)
                + lit(COMMUNICATION_THRESHOLD_MINUTES),
            ).otherwise(
                (
                    unix_timestamp(lit(to_utc_str))
                    - unix_timestamp(col("TimeSpan"))
                )
                / lit(60)
            )
        ),
    ).withColumn(
        "IsEventCompleted",
        when(col("next_event_time").isNotNull(), lit(True))
        .when(
            unix_timestamp(lit(to_utc_str))
            - unix_timestamp(col("last_event_time"))
            >= lit(COMMUNICATION_THRESHOLD_MINUTES * 60),
            lit(True),
        )
        .otherwise(lit(False)),
    )

    if previous_completed_events_df is not None:
        result = result.withColumn(
            "sensor_event_row_number",
            row_number().over(change_window),
        )
        first_events = result.filter(col("sensor_event_row_number") == lit(1)).alias(
            "current"
        )
        previous_events = previous_completed_events_df.alias("previous")
        previous_event_window = Window.partitionBy(
            col("current.SensorId"),
            col("current.TimeSpan"),
        ).orderBy(col("previous.PreviousTimeSpan").desc_nulls_last())
        first_events = (
            first_events
            .join(
                previous_events,
                (col("current.SensorId") == col("previous.SensorId"))
                & (
                    col("previous.PreviousTimeSpan")
                    < col("current.TimeSpan")
                )
                & (
                    col("current.TimeSpan")
                    < col("previous.PreviousEndTime")
                ),
                "left",
            )
            .withColumn(
                "previous_event_row_number",
                row_number().over(previous_event_window),
            )
            .filter(col("previous_event_row_number") == lit(1))
            .filter(
                ~(
                    col("previous.PreviousTimeSpan").isNotNull()
                    & (col("current.Value") == col("previous.PreviousValue"))
                    & col("current.IsEventCompleted")
                    & (
                        unix_timestamp(col("current.TimeSpan"))
                        + spark_round(col("current.EventMinutes") * lit(60)).cast(
                            "long"
                        )
                        == unix_timestamp(col("previous.PreviousEndTime"))
                    )
                )
            )
            .select("current.*")
        )
        result = (
            result
            .filter(col("sensor_event_row_number") > lit(1))
            .unionByName(first_events)
            .drop("sensor_event_row_number")
        )

    result = (
        result
        .withColumn(
            "event_hash",
            md5(
                concat(
                    lit("event"),
                    col("SensorId").cast("string"),
                    col("TimeSpan").cast("string"),
                )
            ),
        )
        .withColumn(
            "EventId",
            concat_ws(
                "-",
                substring("event_hash", 1, 8),
                substring("event_hash", 9, 4),
                substring("event_hash", 13, 4),
                substring("event_hash", 17, 4),
                substring("event_hash", 21, 12),
            ),
        )
        .withColumn(
            "LocalTimeSpan",
            from_utc_timestamp(col("TimeSpan"), col("TimeZone")),
        )
    )

    return result.select(
        col("EventId"),
        col("ReadId"),
        col("SensorId"),
        col("DeviceId"),
        col("LocationId"),
        col("sublocation_name").alias("LocationName"),
        col("device_type").alias("DeviceType"),
        col("sensor_type").alias("SensorType"),
        col("sensor_name").alias("SensorName"),
        col("TimeSpan"),
        col("LocalTimeSpan"),
        col("Value"),
        col("EventMinutes"),
        col("IsEventCompleted"),
    )
