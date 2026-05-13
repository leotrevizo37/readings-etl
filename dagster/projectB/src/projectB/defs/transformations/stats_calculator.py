from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    date_trunc,
    avg,
    lit,
    sum as spark_sum,
    count,
    when as spark_when,
    stddev)

def stats_calculator(
        base: DataFrame,
        sensor_type: Optional[str] = "Corriente",
        min_expected_value: Optional[float] = None,
        max_expected_value: Optional[float] = None
) -> Optional[DataFrame]:

    base = (
        base
        .filter((col("sensor_type") == sensor_type))
        .withColumn("HourStart", date_trunc("hour", col("LocalTimeSpan")))
    )

    agg_exprs = [
        avg("Value").alias("Val_dummy"),
        count("*").alias("total_readings"),
        stddev("Value").alias("Val_std"),
    ]

    anomaly_cond = None
    if min_expected_value is not None:
        anomaly_cond = (col("Value") < lit(min_expected_value))
    if max_expected_value is not None:
        max_cond = (col("Value") > lit(max_expected_value))
        if anomaly_cond is not None:
            anomaly_cond = anomaly_cond | max_cond
        else:
            anomaly_cond = max_cond

    if anomaly_cond is not None:
        agg_exprs.append(
            spark_sum(spark_when(anomaly_cond, 1).otherwise(0)).alias("out_of_range_count")
        )

    stats_df = (
        base
        .groupBy(
            "DeviceId",
            "HourStart",
            "SensorId",
            "LocationId",
            "SensorTyId",
            "sensor_type",
            "device_type",
            "device_name"
        ).agg(*agg_exprs)
        .withColumnRenamed("total_readings", "ReadingsCount")
    )

    if anomaly_cond is not None:
        stats_df = stats_df.withColumn(
            "Anomalies",
            spark_when(col("out_of_range_count") > 0, lit(1)).otherwise(lit(0))
        ).drop("out_of_range_count")
    else:
        stats_df = stats_df.withColumn("Anomalies", lit(0).cast("integer"))

    stats_current_df = (
        stats_df
        .withColumnRenamed("Val_dummy", "MeasurementValue")
        .withColumnRenamed("LocalTimeSpan", "_LocalTimeSpan")
        .withColumnRenamed("HourStart", "LocalTimeSpan")
        .withColumnRenamed("Val_std", "MeasurementStdDev")
    )

    if stats_current_df.rdd.isEmpty():
        return None

    return stats_current_df