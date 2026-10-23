from datetime import datetime, timedelta
from time import perf_counter
from typing import Any, Dict, Optional

from dagster import In, MaterializeResult, Out, RetryPolicy, RetryRequested, graph_asset, op
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from ..resources import SparkResource, SqlServerResource, TransformationOps
from ..transformations import datalake_readings_extended
from ..transformations.sensor_events import resolve_sensor_events_from

SENSOR_EVENTS_MAX_RETRIES = 3
SENSOR_EVENTS_RETRY_DELAY_SECONDS = 60
SENSOR_EVENTS_RETRY_POLICY = RetryPolicy(
    max_retries=SENSOR_EVENTS_MAX_RETRIES,
    delay=SENSOR_EVENTS_RETRY_DELAY_SECONDS,
)

INGESTION_NAME = "FactSensorEvents"
PRESENCE_SENSOR_TYPE = "%presencia%"
LIGHT_SENSOR_TYPE = "%control de luces%"
TEMPERATURE_SENSOR_TYPE = "Temperatura"
TEMPERATURE_EVENTS_LOOKBACK_HOURS = 2


def _truncate_to_seconds(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


def _resolve_temperature_events_from(
    control_from: datetime,
    open_event_from: Optional[datetime],
) -> datetime:
    lookback_from = control_from - timedelta(hours=TEMPERATURE_EVENTS_LOOKBACK_HOURS)
    if open_event_from is None:
        return lookback_from
    return min(lookback_from, open_event_from)


@op(
    required_resource_keys={"sqlserver"},
    ins={},
    out=Out(Dict[str, Any]),
    retry_policy=SENSOR_EVENTS_RETRY_POLICY,
)
def start_run_and_get_range_opff(context) -> Dict[str, Any]:
    start_time = perf_counter()
    sql: SqlServerResource = context.resources.sqlserver

    last_loaded_at = sql.get_watermark(INGESTION_NAME)
    if last_loaded_at is None:
        last_loaded_at = datetime(2026, 1, 16)

    open_event_from = sql.get_min_open_sensor_event_time(PRESENCE_SENSOR_TYPE)
    from_utc = _truncate_to_seconds(
        resolve_sensor_events_from(last_loaded_at, open_event_from)
    )
    light_open_event_from = sql.get_min_open_sensor_event_time(LIGHT_SENSOR_TYPE)
    light_from_utc = _truncate_to_seconds(
        resolve_sensor_events_from(last_loaded_at, light_open_event_from)
    )
    temperature_open_event_from = sql.get_min_open_sensor_event_time(
        TEMPERATURE_SENSOR_TYPE
    )
    temperature_from_utc = _truncate_to_seconds(
        _resolve_temperature_events_from(from_utc, temperature_open_event_from)
    )
    to_utc = _truncate_to_seconds(datetime.utcnow())

    sql.mark_run_started(INGESTION_NAME)

    context.log.info(
        INGESTION_NAME
        + " run window from "
        + str(from_utc)
        + " to "
        + str(to_utc)
        + ". Open event from "
        + str(open_event_from)
        + ". Light window from "
        + str(light_from_utc)
        + ". Light open event from "
        + str(light_open_event_from)
        + ". Temperature window from "
        + str(temperature_from_utc)
        + ". Temperature open event from "
        + str(temperature_open_event_from)
    )

    return {
        "from_utc": from_utc,
        "to_utc": to_utc,
        "original_last_loaded_at": last_loaded_at,
        "open_event_from": open_event_from,
        "light_from_utc": light_from_utc,
        "light_open_event_from": light_open_event_from,
        "temperature_from_utc": temperature_from_utc,
        "temperature_open_event_from": temperature_open_event_from,
        "start_time": start_time,
    }


@op(
    required_resource_keys={"sqlserver", "spark", "transformation_ops"},
    ins={"window": In(Dict[str, Any])},
    out=Out(Dict[str, Any]),
    retry_policy=SENSOR_EVENTS_RETRY_POLICY,
)
def transform_and_load_sensor_events_op(context, window: Dict[str, Any]) -> Dict[str, Any]:
    sql: SqlServerResource = context.resources.sqlserver
    spark_res: SparkResource = context.resources.spark
    transformation_ops: TransformationOps = context.resources.transformation_ops

    from_utc: datetime = window["from_utc"]
    light_from_utc: datetime = window["light_from_utc"]
    temperature_from_utc: datetime = window["temperature_from_utc"]
    to_utc: datetime = window["to_utc"]
    original_last_loaded_at: datetime = window["original_last_loaded_at"]
    run_id = context.run_id

    succeeded = False
    error_message: Optional[str] = None
    max_time_span: Optional[datetime] = None
    staged_rows = 0
    row_count = 0
    retry_number = getattr(context, "retry_number", 0)

    events_df: Optional[DataFrame] = None
    readings_extended_df: Optional[DataFrame] = None
    try:
        if retry_number > 0:
            cleared_rows = sql.clear_fact_sensor_events_stage(run_id)
            context.log.info(
                "Cleared "
                + str(cleared_rows)
                + " factSensorEvents stage rows before retry."
            )

        dimensions_df = spark_res.read_dim_prod_dimensions()
        readings_from = min(from_utc, temperature_from_utc, light_from_utc)
        readings_extended_df = datalake_readings_extended(
            spark_res=spark_res,
            raw_dimensions=dimensions_df,
            fetch_readings_from=readings_from,
            fetch_readings_to=to_utc,
            use_utc_range=True,
        )

        if readings_extended_df is None:
            context.log.info("No extended readings found after joining dimensions.")
            succeeded = True
        else:
            readings_extended_df = readings_extended_df.localCheckpoint(
                eager=True,
                storageLevel=StorageLevel.MEMORY_AND_DISK,
            )
            to_utc_str = to_utc.strftime("%Y-%m-%d %H:%M:%S")
            presence_extended_df = readings_extended_df.filter(
                col("TimeSpan") >= from_utc
            )
            light_extended_df = readings_extended_df.filter(
                col("TimeSpan") >= light_from_utc
            )
            temperature_extended_df = readings_extended_df.filter(
                col("TimeSpan") >= temperature_from_utc
            )
            previous_completed_events_df = (
                spark_res.read_completed_sensor_events_range(
                    PRESENCE_SENSOR_TYPE,
                    from_utc,
                    to_utc,
                )
            )
            expected_values_df = spark_res.read_dim_sensor_expected_values()
            previous_completed_light_events_df = (
                spark_res.read_completed_sensor_events_range(
                    LIGHT_SENSOR_TYPE,
                    light_from_utc,
                    to_utc,
                )
            )
            transformations = [
                (
                    "sensor_events",
                    (
                        presence_extended_df,
                        to_utc_str,
                        previous_completed_events_df,
                    ),
                ),
                (
                    "light_events",
                    (
                        light_extended_df,
                        to_utc_str,
                        previous_completed_light_events_df,
                    ),
                ),
                (
                    "temperature_events",
                    (temperature_extended_df, expected_values_df, to_utc_str),
                ),
            ]

            for method, args in transformations:
                context.log.info("Materializing dataframe for method: " + method)
                fn = getattr(transformation_ops, method)
                df = fn(*args)
                if df is None:
                    context.log.info("No dataframe returned for method: " + method)
                    continue
                if events_df is None:
                    events_df = df
                else:
                    events_df = events_df.unionByName(df)

            if events_df is None:
                context.log.info("No rows across sensor event transformations.")
                readings_extended_df.unpersist(blocking=True)
                readings_extended_df = None
                succeeded = True
                row_count = 0
                max_time_span = None
            else:
                events_df = events_df.persist(StorageLevel.MEMORY_AND_DISK)
                staged_rows = spark_res.write_fact_sensor_events(events_df, run_id)
                row_count = sql.merge_fact_sensor_events(run_id)
                readings_extended_df.unpersist(blocking=True)
                readings_extended_df = None
                if row_count > 0:
                    max_time_span = spark_res.get_max_time_span(
                        events_df,
                        "TimeSpan",
                    )
                context.log.info(
                    str(staged_rows)
                    + " sensor event rows staged and "
                    + str(row_count)
                    + " rows merged into dwh.factSensorEvents. "
                    + "Max TimeSpan "
                    + str(max_time_span)
                )
                succeeded = True
    except Exception as ex:
        error_message = str(ex)
        context.log.error("Error loading FactSensorEvents window: " + error_message)
        try:
            cleared_rows = sql.clear_fact_sensor_events_stage(run_id)
            context.log.warning(
                "Cleared %s factSensorEvents stage rows after failed write.",
                cleared_rows,
            )
        except Exception as cleanup_ex:
            context.log.warning(
                "Could not clean factSensorEvents staging rows before retry: %s",
                cleanup_ex,
            )
        if retry_number < SENSOR_EVENTS_MAX_RETRIES:
            context.log.warning(
                "FactSensorEvents window load failed on attempt %s of %s. Retrying operation.",
                retry_number + 1,
                SENSOR_EVENTS_MAX_RETRIES + 1,
            )
            raise RetryRequested(
                max_retries=SENSOR_EVENTS_MAX_RETRIES,
                seconds_to_wait=SENSOR_EVENTS_RETRY_DELAY_SECONDS,
            ) from ex
        succeeded = False
    finally:
        if events_df is not None:
            events_df.unpersist()
        if readings_extended_df is not None:
            readings_extended_df.unpersist()
        try:
            sql.clear_fact_sensor_events_stage(run_id)
        except Exception:
            pass

    return {
        "from_utc": from_utc,
        "to_utc": to_utc,
        "original_last_loaded_at": original_last_loaded_at,
        "open_event_from": window.get("open_event_from"),
        "light_from_utc": light_from_utc,
        "light_open_event_from": window.get("light_open_event_from"),
        "temperature_from_utc": temperature_from_utc,
        "temperature_open_event_from": window.get("temperature_open_event_from"),
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
    retry_policy=SENSOR_EVENTS_RETRY_POLICY,
)
def finalize_run_opff(context, result: Dict[str, Any]):
    sql: SqlServerResource = context.resources.sqlserver

    to_utc: datetime = result["to_utc"]
    original_last_loaded_at: datetime = result["original_last_loaded_at"]
    max_time_span: Optional[datetime] = result["max_time_span"]
    succeeded: bool = result["succeeded"]
    error_message: Optional[str] = result["error_message"]

    if not succeeded:
        new_watermark = original_last_loaded_at
        context.log.warning(
            INGESTION_NAME + " run failed. Keeping watermark at " + str(new_watermark)
        )
    else:
        if max_time_span is None:
            new_watermark = to_utc
            context.log.info(
                INGESTION_NAME
                + " run succeeded with no events. Advancing watermark to "
                + str(new_watermark)
            )
        else:
            new_watermark = max_time_span
            if new_watermark < original_last_loaded_at:
                new_watermark = original_last_loaded_at
            context.log.info(
                INGESTION_NAME
                + " run succeeded. Advancing watermark to "
                + str(new_watermark)
            )

    sql.mark_run_completed(
        ingestion_name=INGESTION_NAME,
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
            "to_utc": str(to_utc),
            "original_last_loaded_at": str(original_last_loaded_at),
            "open_event_from": str(result.get("open_event_from")),
            "light_from_utc": str(result.get("light_from_utc")),
            "light_open_event_from": str(result.get("light_open_event_from")),
            "temperature_from_utc": str(result.get("temperature_from_utc")),
            "temperature_open_event_from": str(
                result.get("temperature_open_event_from")
            ),
        }
    )


@graph_asset(
    name="dwh_factsensorevents",
    group_name="projectD",
    key_prefix=["projectD"],
)
def dwh_factsensorevents():
    window = start_run_and_get_range_opff()
    result = transform_and_load_sensor_events_op(window)
    return finalize_run_opff(result)
