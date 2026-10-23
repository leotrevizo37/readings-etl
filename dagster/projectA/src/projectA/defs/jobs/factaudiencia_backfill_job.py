from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from dagster import RetryPolicy, daily_partitioned_config, job, op
from pyspark import StorageLevel
from pyspark.sql.functions import col

from ...factaudiencia_backfill import (
    build_backfill_window,
    build_daily_backfill_window,
    get_factaudiencia_backfill_start_datetime,
)
from ..resources import SparkResource, SqlServerResource


FACTAUDIENCIA_BACKFILL_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)


@daily_partitioned_config(start_date=get_factaudiencia_backfill_start_datetime())
def fact_audiencia_backfill_daily_config(start: datetime, _end: datetime):
    window = build_daily_backfill_window(start.strftime("%Y-%m-%d"))
    return {
        "ops": {
            "backfill_factaudiencia_daily_op": {
                "config": {
                    "from_utc": window["from_utc"].isoformat(),
                    "to_utc": window["to_utc"].isoformat(),
                }
            }
        }
    }


def _load_factaudiencia_window(
    context,
    from_utc: datetime,
    to_utc: datetime,
    delete_before_load: bool = False,
) -> Dict[str, Any]:
    sql: SqlServerResource = context.resources.sqlserver
    spark_res: SparkResource = context.resources.spark

    deleted_rows = 0
    if delete_before_load:
        deleted_rows = sql.delete_range(
            schema_name="dwh",
            table_name="FactAudiencia",
            time_column="TimeSpan",
            from_utc=from_utc,
            to_utc=to_utc,
        )

    df = spark_res.read_audiencia_range(
        from_utc,
        to_utc,
    )
    df = df.filter(col("TimeSpan") < to_utc)
    df = df.persist(StorageLevel.MEMORY_AND_DISK)

    row_count = 0
    max_time_span = None
    try:
        if len(df.head(1)) == 0:
            context.log.info(
                "No FactAudiencia rows found for backfill window %s to %s.",
                from_utc,
                to_utc,
            )
        else:
            row_count = spark_res.write_fact_audiencia_to_sql(df)
            max_time_span = spark_res.get_max_time_span(df, "TimeSpan")
            context.log.info(
                "FactAudiencia backfill wrote %s rows. Max TimeSpan %s.",
                row_count,
                max_time_span,
            )
    finally:
        df.unpersist()

    return {
        "from_utc": from_utc,
        "to_utc": to_utc,
        "deleted_rows": deleted_rows,
        "row_count": row_count,
        "max_time_span": max_time_span,
        "delete_before_load": delete_before_load,
    }

@op(
    required_resource_keys={"sqlserver", "spark"},
    config_schema={"from_utc": str, "to_utc": str},
    retry_policy=FACTAUDIENCIA_BACKFILL_RETRY_POLICY,
)
def backfill_factaudiencia_daily_op(context) -> Dict[str, Any]:
    window = build_backfill_window(
        context.op_config["from_utc"],
        context.op_config["to_utc"],
    )
    return _load_factaudiencia_window(
        context,
        window["from_utc"],
        window["to_utc"],
        delete_before_load=True,
    )


@job(
    name="015_Sensor_audiencia_backfill_daily",
    config=fact_audiencia_backfill_daily_config,
    tags={"job_type": "lecturas", "run_lane": "operational", "user_code_run_lane": "projectA_operational", "user_code": "projectA", "backfill": "factaudiencia_daily"},
)
def fact_audiencia_backfill_daily_job():
    backfill_factaudiencia_daily_op()
