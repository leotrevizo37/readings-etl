from datetime import datetime
from typing import Any, Dict, Optional

from dagster import In, Out, job, op

from ..resources import SqlServerResource, SparkResource, sqlserver_resource, spark_resource


def _truncate_to_seconds(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


@op(
    required_resource_keys={"sqlserver"},
    ins={},
    out=Out(Dict[str, Any]),
)
def start_run_and_get_range_op(context) -> Dict[str, Any]:
    sql: SqlServerResource = context.resources.sqlserver
    ingestion_name = "readings"

    last_loaded_at = sql.get_watermark(ingestion_name)
    if last_loaded_at is None:
        last_loaded_at = datetime(2025, 10, 1)

    from_utc = _truncate_to_seconds(last_loaded_at)
    now_utc = _truncate_to_seconds(datetime.utcnow())

    sql.mark_run_started(ingestion_name)

    context.log.info( ingestion_name + "run window from " + str(from_utc) + " to " + str(now_utc))

    return {
        "from_utc": from_utc,
        "to_utc": now_utc,
        "original_last_loaded_at": last_loaded_at,
    }


@op(
    required_resource_keys={"sqlserver", "spark"},
    ins={"window": In(Dict[str, Any])},
    out=Out(Dict[str, Any]),
)
def delete_and_load_window_op(context, window: Dict[str, Any]) -> Dict[str, Any]:
    sql: SqlServerResource = context.resources.sqlserver
    spark_res: SparkResource = context.resources.spark

    from_utc: datetime = window["from_utc"]
    to_utc: datetime = window["to_utc"]
    original_last_loaded_at: datetime = window["original_last_loaded_at"]

    sql.delete_range(
        schema_name="dwh",
        table_name="readings",
        time_column="TimeSpan",
        from_utc=from_utc,
        to_utc=to_utc
    )

    succeeded = False
    error_message: Optional[str] = None
    max_time_span: Optional[datetime] = None

    try:
        df = spark_res.read_readings_range(from_utc, to_utc)
        if df.rdd.isEmpty():
            context.log.info("No rows found in Data Lake for window " + str(from_utc) + " to " + str(to_utc))
            succeeded = True
        else:
            spark_res.write_fact_readings_to_sql(df)
            max_time_span = spark_res.get_max_time_span(df, "TimeSpan")
            context.log.info("Window loaded into dwh.readings. Max TimeSpan " + str(max_time_span))
            succeeded = True
    except Exception as ex:
        error_message = str(ex)
        context.log.error("Error loading readings window: " + error_message)
        succeeded = False

    return {
        "from_utc": from_utc,
        "to_utc": to_utc,
        "original_last_loaded_at": original_last_loaded_at,
        "max_time_span": max_time_span,
        "succeeded": succeeded,
        "error_message": error_message,
    }


@op(
    required_resource_keys={"sqlserver"},
    ins={"result": In(Dict[str, Any])},
    out=Out(),
)
def finalize_run_op(context, result: Dict[str, Any]) -> None:
    sql: SqlServerResource = context.resources.sqlserver
    ingestion_name = "readings"

    to_utc: datetime = result["to_utc"]
    original_last_loaded_at: datetime = result["original_last_loaded_at"]
    max_time_span: Optional[datetime] = result["max_time_span"]
    succeeded: bool = result["succeeded"]
    error_message: Optional[str] = result["error_message"]

    if not succeeded:
        new_watermark = original_last_loaded_at
        context.log.warning(
            "readings run failed. Keeping watermark at " + str(new_watermark)
        )
    else:
        if max_time_span is None:
            new_watermark = to_utc
            context.log.info(
                "readings run succeeded with no rows. Advancing watermark to " + str(new_watermark)
            )
        else:
            new_watermark = max_time_span
            context.log.info(
                "readings run succeeded. Advancing watermark to " + str(new_watermark)
            )

    sql.mark_run_completed(
        ingestion_name=ingestion_name,
        new_last_loaded_at=new_watermark,
        succeeded=succeeded,
        error_message=error_message,
    )


@job(
    resource_defs={
        "sqlserver": sqlserver_resource,
        "spark": spark_resource,
    }
)
def readings_ingestion_job():
    window = start_run_and_get_range_op()
    result = delete_and_load_window_op(window)
    finalize_run_op(result)
