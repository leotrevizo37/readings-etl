from typing import Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    col,
    collect_list,
    date_trunc,
    avg,
    lit,
    max as spark_max,
    min as spark_min,
    sort_array,
    struct,
    sum as spark_sum,
    count,
    to_json,
    when as spark_when,
    stddev)


SENSOR_TYPE_EQUIVALENCES = {
    "Temperatura": ("Temperatura", "Temperature",),
    "Humedad": ("Humedad", "Humidity", "Umidade",),
    "Medicion de corriente": ("Medicion de corriente", "Current Measurement", "Current measurement", "Medição de corrente", "Medição de Corrente", "Medicao de corrente",),
}


def sensor_type_filter(*sensor_types: str, column: Optional[Column] = None) -> Column:
    equivalents = tuple(
        value
        for sensor_type in sensor_types
        for value in SENSOR_TYPE_EQUIVALENCES.get(sensor_type, (sensor_type,))
    )
    if column is None:
        column = col("sensor_type")
    return column.isin(*equivalents)


def stats_calculator(
        base: DataFrame,
        sensor_type: Optional[str] = "Corriente",
        min_expected_value: Optional[float] = None,
        max_expected_value: Optional[float] = None
) -> Optional[DataFrame]:

    base = (
        base
        .filter(sensor_type_filter(sensor_type))
        .withColumn("HourStart", date_trunc("hour", col("TimeSpan")))
    )

    agg_exprs = [
        avg("Value").alias("Val_dummy"),
        spark_min("Value").alias("Min"),
        spark_max("Value").alias("Max"),
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

    if anomaly_cond is None:
        anomaly_cond = lit(False)

    reading_data = struct(
        col("TimeSpan").alias("timestamp"),
        col("Value").alias("value"),
    )
    agg_exprs.extend(
        [
            spark_sum(spark_when(anomaly_cond, 1).otherwise(0))
            .cast("integer")
            .alias("Anomalies"),
            to_json(
                sort_array(
                    collect_list(
                        spark_when(~anomaly_cond, reading_data)
                    )
                )
            ).alias("CorrectReadingsJson"),
            to_json(
                sort_array(
                    collect_list(
                        spark_when(anomaly_cond, reading_data)
                    )
                )
            ).alias("AnomalousReadingsJson"),
        ]
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

    stats_df = stats_df.withColumn(
        "HasAnomalies",
        col("Anomalies") > lit(0),
    )

    stats_current_df = (
        stats_df
        .withColumnRenamed("Val_dummy", "MeasurementValue")
        .withColumnRenamed("HourStart", "TimeSpan")
        .withColumnRenamed("Val_std", "MeasurementStdDev")
    )

    if len(stats_current_df.head(1)) == 0:
        return None

    return stats_current_df
