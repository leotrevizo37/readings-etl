from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (col,
                                   date_trunc,
                                   avg,
                                   sum as spark_sum,
                                   count,
                                   when as spark_when,
                                   unix_timestamp,
                                   from_unixtime,
                                   floor)

def stats_calculator(
        base: DataFrame,
        sensor_type: str = "Corriente",
        min_expected_value: int = 0,
        max_expected_value: Optional[int] = None
) -> Optional[DataFrame]:

    if max_expected_value is None:
        base = (
            base
            .filter(
                (col("_sensor_type") == sensor_type)
            )
            .withColumn("HourStart", date_trunc("hour", col("LocalTimeSpan")))
            .withColumn(
                "5MinuteStart",
                from_unixtime(
                    floor(unix_timestamp(col("LocalTimeSpan")) / 300) * 300
                ).cast("timestamp")
            )
            .withColumn(
                "is_anomaly",
                spark_when(
                    col("Value") < min_expected_value, 1
                ).otherwise(0)
            )
        )

        stats_df = (
            base
            .groupBy("DeviceId", "HourStart", "LocationId", "SensorTyId", "_sensor_type")
            .agg(
                avg("Value").alias("Val_dummy"),
                spark_sum(
                    spark_when(col("Value") >= min_expected_value, 1).otherwise(0)
                ).alias("is_okay"),
                count("*").alias("total_readings"),
                spark_sum("is_anomaly").alias("Anomalies_raw"),
            )
            .withColumn(
                "StatusCode",
                spark_when(col("is_okay") / col("total_readings") >= 0.50, 1).otherwise(3)
            )
            .withColumn("Promedio", col("is_okay") / col("total_readings"))
            .withColumnRenamed("Anomalies_raw", "Anomalies")
        )
    else:
        base = (
            base
            .filter(
                (col("_sensor_type") == sensor_type)
            )
            .withColumn("HourStart", date_trunc("hour", col("LocalTimeSpan")))
            .withColumn(
                "5MinuteStart",
                from_unixtime(
                    floor(unix_timestamp(col("LocalTimeSpan")) / 300) * 300
                ).cast("timestamp")
            )
            .withColumn(
                "is_anomaly",
                spark_when(
                    (col("Value") < min_expected_value) |
                    (col("Value") > max_expected_value), 1
                ).otherwise(0)
            )
        )

        stats_df = (
            base
            .groupBy("DeviceId", "HourStart", "LocationId", "SensorTyId", "_sensor_type")
            .agg(
                avg("Value").alias("Val_dummy"),
                spark_sum(
                    spark_when(
                        (col("Value") >= min_expected_value) &
                        (col("Value") <= max_expected_value), 1).otherwise(0)
                ).alias("is_okay"),
                count("*").alias("total_readings"),
                spark_sum("is_anomaly").alias("Anomalies_raw"),
            )
            .withColumn(
                "StatusCode",
                spark_when(col("is_okay") / col("total_readings") >= 0.50, 1).otherwise(3)
            )
            .withColumn("Promedio", col("is_okay") / col("total_readings"))
            .withColumnRenamed("Anomalies_raw", "Anomalies")
        )

    stats_current_df = (
        stats_df
        .fillna({"Anomalies": 0})
        .withColumnRenamed("Val_dummy", "MeasurementValue")
        .withColumnRenamed("LocalTimeSpan", "_LocalTimeSpan")
        .withColumnRenamed("HourStart", "LocalTimeSpan")
    )

    if stats_current_df.rdd.isEmpty():
        return None

    return stats_current_df