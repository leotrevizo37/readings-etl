from datetime import datetime
from typing import Any, Dict, Optional

from dagster import In, Out, op, graph_asset, MaterializeResult
from pyspark import StorageLevel
from time import perf_counter

from ..resources import SqlServerResource, SparkResource


def _truncate_to_seconds(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


@op(
    required_resource_keys={"sqlserver"},
    ins={},
    out=Out(Dict[str, Any]),
)
def start_run_and_get_range_op(context) -> Dict[str, Any]:
    start_time = perf_counter()
    sql: SqlServerResource = context.resources.sqlserver
    ingestion_name = "FactLecturas"

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
        "start_time": start_time,
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
        table_name="FactLecturas",
        time_column="TimeSpan",
        from_utc=from_utc,
        to_utc=to_utc
    )

    succeeded = False
    error_message: Optional[str] = None
    max_time_span: Optional[datetime] = None
    row_count = 0

    try:
        df = spark_res.read_lecturas_range(from_utc, to_utc)
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        if df.rdd.isEmpty():
            context.log.info("No rows found in Data Lake for window " + str(from_utc) + " to " + str(to_utc))
            succeeded = True
        else:
            row_count = spark_res.write_fact_lecturas_to_sql(df)
            max_time_span = spark_res.get_max_time_span(df, "TimeSpan")
            context.log.info("Window loaded into dwh.factLecturas. Max TimeSpan " + str(max_time_span))
            succeeded = True
    except Exception as ex:
        error_message = str(ex)
        context.log.error("Error loading FactLecturas window: " + error_message)
        succeeded = False

    return {
        "from_utc": from_utc,
        "to_utc": to_utc,
        "original_last_loaded_at": original_last_loaded_at,
        "max_time_span": max_time_span,
        "succeeded": succeeded,
        "error_message": error_message,
        "row_count": row_count,
        "start_time": window.get("start_time"),
    }


@op(
    required_resource_keys={"sqlserver"},
    ins={"result": In(Dict[str, Any])},
    out=Out(),
)
def finalize_run_op(context, result: Dict[str, Any]):
    sql: SqlServerResource = context.resources.sqlserver
    ingestion_name = "FactLecturas"

    to_utc: datetime = result["to_utc"]
    original_last_loaded_at: datetime = result["original_last_loaded_at"]
    max_time_span: Optional[datetime] = result["max_time_span"]
    succeeded: bool = result["succeeded"]
    error_message: Optional[str] = result["error_message"]

    if not succeeded:
        new_watermark = original_last_loaded_at
        context.log.warning(
            "FactLecturas run failed. Keeping watermark at " + str(new_watermark)
        )
    else:
        if max_time_span is None:
            new_watermark = to_utc
            context.log.info(
                "FactLecturas run succeeded with no rows. Advancing watermark to " + str(new_watermark)
            )
        else:
            new_watermark = max_time_span
            context.log.info(
                "FactLecturas run succeeded. Advancing watermark to " + str(new_watermark)
            )

    sql.mark_run_completed(
        ingestion_name=ingestion_name,
        new_last_loaded_at=new_watermark,
        succeeded=succeeded,
        error_message=error_message,
    )

    start_time = result.get("start_time")
    elapsed = round(perf_counter() - start_time, 3) if start_time else 0.0

    return MaterializeResult(
        metadata={
            "row_count": result.get("row_count", 0),
            "elapsed_seconds": elapsed,
        }
    )


@graph_asset(name="dwh_factlecturas", group_name="projectC", key_prefix=["projectC"])
def dwh_factlecturas():
    window = start_run_and_get_range_op()
    result = delete_and_load_window_op(window)
    return finalize_run_op(result)
