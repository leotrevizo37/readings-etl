from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    max as spark_max,
    when,
    lit,
)

def factReadingsMeasurement_device(
        factoperations_df: DataFrame) -> DataFrame:

    grouped = (
        factoperations_df
        .groupBy("OperationId", "DeviceId", "LocationId", "LocalTimeSpan")
        .agg(
            spark_sum("Anomalies").alias("Anomalies"),

            spark_max(
                when(
                    (col("_sensor_type") == "Corriente") & (col("StatusCode") == 1),
                    lit(1),
                )
                .when(
                    (col("_sensor_type") == "Corriente") & (col("StatusCode") == 3),
                    lit(0),
                )
                .otherwise(lit(None))
            ).alias("amperage_status"),

            spark_max(
                when(
                    (col("_sensor_type") == "Presión") & (col("StatusCode") == 1),
                    lit(1),
                )
                .when(
                    (col("_sensor_type") == "Presión") & (col("StatusCode") == 3),
                    lit(0),
                )
                .otherwise(lit(None))
            ).alias("pressure_status"),

            spark_max(
                when(
                    (col("_sensor_type") == "Temperatura") & (col("StatusCode") == 1),
                    lit(1),
                )
                .when(
                    (col("_sensor_type") == "Temperatura") & (col("StatusCode") == 3),
                    lit(0),
                )
                .otherwise(lit(None))
            ).alias("temperature_status"),

            spark_max(
                when(
                    (col("_sensor_type") == "Encendido") & (col("StatusCode") == 1),
                    lit(1),
                )
                .when(
                    (col("_sensor_type") == "Encendido") & (col("StatusCode") == 3),
                    lit(0),
                )
                .otherwise(lit(None))
            ).alias("current_status"),

            spark_max(
                when(
                    (col("_sensor_type") == "On/Off Termostato") & (col("StatusCode") == 1),
                    lit(1),
                )
                .when(
                    (col("_sensor_type") == "On/Off Termostato") & (col("StatusCode") == 3),
                    lit(0),
                )
                .otherwise(lit(None))
            ).alias("termostatus"),
        )
    )

    result = (
        grouped
        .withColumn(
            "StatusCode",
            when(
                (col("amperage_status") == 1)
                & col("pressure_status").isNull()
                & col("temperature_status").isNull()
                & col("current_status").isNull()
                & col("termostatus").isNull(),
                lit(1),
            )
            .when(
                (col("amperage_status") == 1)
                & (col("pressure_status") == 1)
                & col("temperature_status").isNull()
                & col("current_status").isNull()
                & col("termostatus").isNull(),
                lit(1),
            )
            .when(
                (col("amperage_status") == 1)
                & (col("pressure_status") == 0)
                & col("temperature_status").isNull()
                & col("current_status").isNull()
                & col("termostatus").isNull(),
                lit(2),
            )
            .when(
                (col("amperage_status") == 1)
                & (col("pressure_status").isNull())
                & (col("temperature_status") == 0)
                & col("current_status").isNull()
                & col("termostatus").isNull(),
                lit(4),
            )
            .when(
                (col("amperage_status") == 1)
                & (col("pressure_status").isNull())
                & (col("temperature_status") == 1)
                & col("current_status").isNull()
                & col("termostatus").isNull(),
                lit(1),
            )
            .when(
                (col("amperage_status").isNull())
                & (col("pressure_status").isNull())
                & (col("temperature_status") == 1)
                & col("current_status").isNull()
                & col("termostatus").isNull(),
                lit(1),
            )
            .when(
                (col("amperage_status").isNull())
                & (col("pressure_status").isNull())
                & (col("temperature_status") == 0)
                & col("current_status").isNull()
                & col("termostatus").isNull(),
                lit(4),
            )
            .when(
                (col("amperage_status").isNull())
                & (col("pressure_status").isNull())
                & (col("temperature_status").isNull())
                & (col("current_status") == 1)
                & col("termostatus").isNull(),
                lit(1),
            )
            .when(
                (col("amperage_status") == 1)
                & (col("pressure_status").isNull())
                & (col("temperature_status").isNull())
                & (col("current_status") == 1)
                & col("termostatus").isNull(),
                lit(1),
            )
            .when(
                (col("amperage_status") == 0)
                & (col("pressure_status").isNull())
                & (col("temperature_status").isNull())
                & (col("current_status") == 1)
                & col("termostatus").isNull(),
                lit(1),
            )
            .when(
                col("amperage_status").isNull()
                & col("pressure_status").isNull()
                & (col("temperature_status") == 1)
                & col("current_status").isNull()
                & (col("termostatus") == 1),
                lit(1),
            )
            .when(
                col("amperage_status").isNull()
                & col("pressure_status").isNull()
                & (col("temperature_status") == 0)
                & col("current_status").isNull()
                & (col("termostatus") == 1),
                lit(1),
            )
            .otherwise(lit(3))
        )
        .select(
            col("DeviceId"),
            col("OperationId"),
            col("LocalTimeSpan"),
            col("StatusCode"),
            col("Anomalies"),
        )
    )

    return result
