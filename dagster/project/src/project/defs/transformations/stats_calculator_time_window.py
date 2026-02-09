from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    date_trunc,
    avg,
    sum as spark_sum,
    count,
    lag,
    lit,
    when as spark_when,
    max as spark_max,
    unix_timestamp,
    from_unixtime,
    floor,
    min as spark_min,
    explode,
    sequence,
    expr,
    coalesce)

from pyspark.sql.window import Window


def stats_calculator_time_window(
    base: DataFrame,
    sensor_type: str = "Corriente",
    min_expected_value: float = 0,
    max_expected_value: Optional[float] = None,
    time_window: Optional[int] = 20
) -> Optional[DataFrame]:
    base = (
        base
        .filter(col("_sensor_type") == sensor_type)
        .withColumn("HourStart", date_trunc("hour", col("LocalTimeSpan")))
        .withColumn(
            "5MinuteStart",
            from_unixtime(
                floor(unix_timestamp(col("LocalTimeSpan")) / 300) * 300
            ).cast("timestamp")
        )
        .withColumn(
            "xMinuteStart",
            from_unixtime(
                floor(unix_timestamp(col("LocalTimeSpan")) / (time_window * 60)) * (time_window * 60)
            ).cast("timestamp")
        )
    )

    w = Window.partitionBy("HourStart").orderBy("LocalTimeSpan")

    is_bad = col("Value") <= lit(min_expected_value)
    if max_expected_value is not None:
        is_bad = is_bad | (col("Value") > lit(max_expected_value))

    base = base.withColumn("is_zero", is_bad.cast("int"))

    prev_is_zero = lag("is_zero", 1).over(w)
    base = base.withColumn(
        "zero_run_start",
        spark_when(
            (col("is_zero") == 1) &
            ((prev_is_zero != 1) | prev_is_zero.isNull()),
            1
        ).otherwise(0)
    )

    base = base.withColumn(
        "zero_group",
        spark_sum("zero_run_start").over(w)
    )

    w_group = Window.partitionBy("HourStart", "zero_group")
    base = base.withColumn(
        "zero_run_len",
        spark_sum("is_zero").over(w_group)
    )

    per_min5_max_df = (
        base
        .groupBy("DeviceId", "5MinuteStart", "SensorId", "LocationId", "SensorTyId", "_sensor_type")
        .agg(
            spark_max("Value").alias("Value_phase_max")
        )
    )

    per5max_df = per_min5_max_df.withColumn(
        "HourStart",
        date_trunc("hour", col("5MinuteStart"))
    )

    per5_device_max_df = (
        per5max_df
        .groupBy("DeviceId", "HourStart", "5MinuteStart", "LocationId", "SensorTyId", "_sensor_type")
        .agg(
            spark_max("Value_phase_max").alias("Value_5m_max")
        )
    )

    ok_cond = col("Value_5m_max") > lit(min_expected_value)
    if max_expected_value is not None:
        ok_cond = ok_cond & (col("Value_5m_max") <= lit(max_expected_value))

    stats_df = (
        per5_device_max_df
        .groupBy("DeviceId", "HourStart")
        .agg(
            avg("Value_5m_max").alias("Val_dummy"),
            spark_sum(
                spark_when(ok_cond, 1).otherwise(0)
            ).alias("is_okay"),
        )
        .withColumn("total_readings", lit(12))
        .withColumn(
            "StatusCode",
            spark_when(
                col("is_okay") / col("total_readings") >= lit(0.3),
                lit(1)
            ).otherwise(lit(3))
        )
    )

    valor_df = (
        per5max_df
        .groupBy("DeviceId", "HourStart", "LocationId", "SensorTyId", "_sensor_type")
        .agg(
            avg("Value_phase_max").alias("MeasurementValue"),
            count("*").alias("total_sensors"),
        )
    )

    agg_20 = (
        base
        .groupBy("DeviceId", "LocationId", "HourStart", "xMinuteStart", "SensorTyId")
        .agg(
            count("*").alias("row_count"),
            spark_sum(col("is_zero").cast("int")).alias("zero_count"),
        )
    )

    device_span = (
        base
        .groupBy("DeviceId", "LocationId", "SensorTyId")
        .agg(
            spark_min("xMinuteStart").alias("min_win"),
            spark_max("xMinuteStart").alias("max_win"),
        )
    )

    all_windows = (
        device_span
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            explode(
                sequence("min_win", "max_win", expr("INTERVAL " + str(time_window) + " MINUTES"))
            ).alias("xMinuteStart"),
        )
    )

    windows_with_data = (
        all_windows
        .join(
            agg_20,
            on=["DeviceId", "LocationId", "xMinuteStart", "SensorTyId"],
            how="left",
        )
        .withColumn("row_count", coalesce(col("row_count"), lit(0)))
        .withColumn("zero_count", coalesce(col("zero_count"), lit(0)))
    )

    windows_with_data = windows_with_data.withColumn(
        "HourStart",
        date_trunc("hour", col("xMinuteStart"))
    )

    min_limit = min_expected_value

    anom_condition = (
        (col("row_count") == 0) |
        ((col("row_count") > 0) & (col("zero_count") == col("row_count")))
    )

    if min_limit is not None:
        anom_condition = anom_condition | (
            (col("row_count") < lit(min_limit)) & (col("zero_count") > 0)
        )
    else:
        anom_condition = anom_condition | (col("zero_count") > 0)

    windows_with_anom = (
        windows_with_data
        .withColumn(
            "Anomalia_x_min",
            spark_when(
                anom_condition,
                lit(1),
            ).otherwise(lit(0)),
        )
    )

    time_window_mins = int(time_window) if time_window is not None else 20
    hours_in_window = int(time_window_mins / 60)

    anomalies_df = (
        windows_with_anom
        .groupBy("DeviceId", "HourStart", "SensorTyId")
        .agg(
            spark_sum("Anomalia_x_min").alias("Anomalies_raw"),
        )
        .withColumn(
            "Anomalies",
            spark_when(
                lit(hours_in_window) > lit(1),
                spark_when(col("Anomalies_raw") == lit(hours_in_window), lit(1)).otherwise(lit(0))
            ).otherwise(
                spark_when(col("Anomalies_raw") > lit(3), lit(3)).otherwise(col("Anomalies_raw"))
            )
        )
        .select("HourStart", "Anomalies", "DeviceId")
    )

    stats_current_df = (
        valor_df
        .join(stats_df.drop("Val_dummy"), on=["HourStart", "DeviceId"], how="inner")
        .join(anomalies_df, on=["HourStart", "DeviceId"], how="left")
        .fillna({"Anomalies": 0})
        .withColumnRenamed("LocalTimeSpan", "_LocalTimeSpan")
        .withColumnRenamed("HourStart", "LocalTimeSpan")
    )

    if stats_current_df.rdd.isEmpty():
        return None

    return stats_current_df
