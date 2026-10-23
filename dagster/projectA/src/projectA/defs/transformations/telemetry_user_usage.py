from datetime import datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    coalesce,
    countDistinct,
    date_add,
    date_sub,
    explode,
    expr,
    lag,
    lit,
    lower,
    max as spark_max,
    sequence,
    sum as spark_sum,
    avg as spark_avg,
    to_date,
    trim,
    when,
)


EXCLUDED_HUMAN_MODULES = [
    "",
    "undefined",
    "reads",
    "batch-activity",
    "loginfailure",
    "aspnetcore-browser-refresh.js",
    "favicon.ico",
]
GUID_MODULE_PATTERN = (
    "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    "[0-9a-f]{4}-[0-9a-f]{12}$"
)


def build_dim_aspnet_users_df(users_df: DataFrame) -> DataFrame:
    return users_df.select(
        "Id",
        "UserName",
        "Name",
        "LastName",
        "Position",
    )


def build_fact_telemetry_user_usage_df(
    users_df: DataFrame,
    system_logs_df: DataFrame,
    from_utc: datetime,
    to_utc: datetime,
) -> DataFrame:
    spark = users_df.sparkSession
    dates_df = (
        spark.range(1)
        .select(
            explode(
                sequence(
                    to_date(lit(from_utc.strftime("%Y-%m-%d"))),
                    date_sub(to_date(lit(to_utc.strftime("%Y-%m-%d"))), 1),
                )
            ).alias("Date")
        )
    )
    users = (
        users_df
        .select(
            col("Id").alias("UserId"),
            col("CreatedAt").alias("_user_created_at"),
        )
        .dropDuplicates(["UserId"])
    )
    user_days = (
        users
        .crossJoin(dates_df)
        .filter(
            col("_user_created_at")
            < date_add(col("Date"), 1).cast("timestamp")
        )
        .select("UserId", "Date")
    )

    successful_events = (
        system_logs_df
        .filter(
            col("UserId").isNotNull()
            & col("StatusCode").between(200, 399)
        )
        .withColumn("Date", to_date(col("RequestDate")))
        .withColumn(
            "_module",
            lower(trim(coalesce(col("Module"), lit("")))),
        )
    )
    connected = successful_events.groupBy("UserId", "Date").agg(
        spark_max(lit(1)).alias("_has_connected")
    )

    human_events = successful_events.filter(
        ~col("_module").isin(*EXCLUDED_HUMAN_MODULES)
        & ~col("_module").rlike(GUID_MODULE_PATTERN)
    )
    functional_modules = (
        human_events
        .filter(col("_module") != "login")
        .groupBy("UserId", "Date")
        .agg(countDistinct("_module").alias("_functional_modules"))
    )

    event_window = Window.partitionBy("UserId", "Date").orderBy(
        col("RequestDate"),
        col("RegisterId"),
    )
    connected_time = (
        human_events
        .withColumn(
            "_previous_request",
            lag("RequestDate").over(event_window),
        )
        .withColumn(
            "_gap_seconds",
            col("RequestDate").cast("long")
            - col("_previous_request").cast("long"),
        )
        .withColumn(
            "_connected_seconds",
            when(
                col("_gap_seconds").between(0, 1800),
                col("_gap_seconds"),
            ).otherwise(lit(0)),
        )
        .groupBy("UserId", "Date")
        .agg(
            spark_sum("_connected_seconds")
            .cast("long")
            .alias("_time_connected")
        )
    )

    response_metrics = (
        human_events
        .filter(
            col("ExecutionTimeMs").isNotNull()
            & (col("ExecutionTimeMs") >= 0)
        )
        .groupBy("UserId", "Date")
        .agg(
            spark_avg(col("ExecutionTimeMs").cast("long"))
            .cast("long")
            .alias("_waiting_time"),
            expr("percentile(ExecutionTimeMs, 0.95)")
            .cast("decimal(18, 2)")
            .alias("Latency95thPercentile"),
        )
    )

    return (
        user_days
        .join(connected, ["UserId", "Date"], "left")
        .join(functional_modules, ["UserId", "Date"], "left")
        .join(connected_time, ["UserId", "Date"], "left")
        .join(response_metrics, ["UserId", "Date"], "left")
        .select(
            "UserId",
            "Date",
            coalesce(col("_has_connected"), lit(0))
            .cast("boolean")
            .alias("HasConnected"),
            (
                (
                    coalesce(col("_functional_modules"), lit(0)) >= 2
                ) & (
                    coalesce(col("_time_connected"), lit(0)) > 10
                )
            ).alias("MadeCompleteInteraction"),
            coalesce(col("_time_connected"), lit(0))
            .cast("long")
            .alias("TimeConnected"),
            coalesce(col("_waiting_time"), lit(0))
            .cast("long")
            .alias("AvgLatency"),
            col("Latency95thPercentile"),
        )
    )
