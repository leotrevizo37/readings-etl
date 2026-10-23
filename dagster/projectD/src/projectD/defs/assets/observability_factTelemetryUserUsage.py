from datetime import datetime
from time import perf_counter

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    RetryPolicy,
    asset,
)
from pyspark import StorageLevel

from .source_assets import dbo_aspnetusers, dbo_systemlogs
from ..resources import SparkResource, SqlDimServerResource, SqlServerResource
from ..transformations.telemetry_user_usage import build_fact_telemetry_user_usage_df


FACT_TELEMETRY_USER_USAGE_INGESTION_NAME = "factTelemetryUserUsage"
FACT_TELEMETRY_USER_USAGE_START_DATE = datetime(2026, 6, 5)
OBSERVABILITY_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)


@asset(
    deps=[dbo_aspnetusers, dbo_systemlogs],
    required_resource_keys={"spark", "dim_sqlserver", "sqlserver"},
    group_name="projectD",
    key_prefix=["projectD"],
    retry_policy=OBSERVABILITY_RETRY_POLICY,
)
def observability_facttelemetryuserusage(
    context: AssetExecutionContext,
) -> MaterializeResult:
    start_time = perf_counter()
    spark_res: SparkResource = context.resources.spark
    dim_sql: SqlDimServerResource = context.resources.dim_sqlserver
    sqlserver: SqlServerResource = context.resources.sqlserver
    run_id = context.run_id

    last_loaded_at = sqlserver.get_watermark(
        FACT_TELEMETRY_USER_USAGE_INGESTION_NAME
    )
    if last_loaded_at is None:
        last_loaded_at = FACT_TELEMETRY_USER_USAGE_START_DATE

    from_utc = datetime(
        last_loaded_at.year,
        last_loaded_at.month,
        last_loaded_at.day,
    )
    now_utc = datetime.utcnow()
    to_utc = datetime(now_utc.year, now_utc.month, now_utc.day)
    sqlserver.mark_run_started(FACT_TELEMETRY_USER_USAGE_INGESTION_NAME)

    staged_rows = 0
    merged_rows = 0
    result_df = None
    try:
        sqlserver.clear_fact_telemetry_user_usage_stage(run_id)
        if from_utc < to_utc:
            spark = spark_res.get_spark()
            from_str = from_utc.strftime("%Y-%m-%dT%H:%M:%S")
            to_str = to_utc.strftime("%Y-%m-%dT%H:%M:%S")
            users_query = (
                "SELECT Id, CreatedAt "
                "FROM dbo.AspNetUsers "
                "WHERE CreatedAt < CAST('" + to_str + "' AS DATETIME2)"
            )
            logs_query = (
                "SELECT "
                "RegisterId, "
                "UserId, "
                "Module, "
                "RequestDate, "
                "TRY_CONVERT(INT, JSON_VALUE("
                "CASE WHEN ISJSON(Body) = 1 THEN Body END, "
                "'$.StatusCode')) AS StatusCode, "
                "TRY_CONVERT(BIGINT, JSON_VALUE("
                "CASE WHEN ISJSON(Body) = 1 THEN Body END, "
                "'$.ExecutionTimeMs')) AS ExecutionTimeMs "
                "FROM dbo.SystemLogs "
                "WHERE RequestDate >= CAST('" + from_str + "' AS DATETIME2) "
                "AND RequestDate < CAST('" + to_str + "' AS DATETIME2)"
            )
            users_df = (
                spark.read
                .format("jdbc")
                .option(
                    "dbtable",
                    "(" + users_query + ") AS usage_users",
                )
                .options(**dim_sql.get_basic_options())
                .load()
            )
            system_logs_df = (
                spark.read
                .format("jdbc")
                .option(
                    "dbtable",
                    "(" + logs_query + ") AS usage_logs",
                )
                .options(**dim_sql.get_basic_options())
                .load()
            )
            result_df = build_fact_telemetry_user_usage_df(
                users_df,
                system_logs_df,
                from_utc,
                to_utc,
            ).persist(StorageLevel.MEMORY_AND_DISK)
            staged_rows = result_df.count()
            if staged_rows > 0:
                spark_res.write_fact_telemetry_user_usage_stage(result_df, run_id)
                merged_rows = sqlserver.merge_fact_telemetry_user_usage(run_id)

    except Exception as ex:
        sqlserver.mark_run_completed(
            ingestion_name=FACT_TELEMETRY_USER_USAGE_INGESTION_NAME,
            new_last_loaded_at=last_loaded_at,
            succeeded=False,
            error_message=str(ex),
        )
        raise
    finally:
        if result_df is not None:
            result_df.unpersist()
        sqlserver.clear_fact_telemetry_user_usage_stage(run_id)

    sqlserver.mark_run_completed(
        ingestion_name=FACT_TELEMETRY_USER_USAGE_INGESTION_NAME,
        new_last_loaded_at=max(to_utc, last_loaded_at),
        succeeded=True,
        error_message=None,
    )

    return MaterializeResult(
        metadata={
            "row_count": staged_rows,
            "merged_rows": merged_rows,
            "elapsed_seconds": round(perf_counter() - start_time, 3),
            "from_utc": str(from_utc),
            "to_utc": str(to_utc),
        },
    )

