import os
from datetime import datetime, timedelta, timezone
from time import perf_counter

from dagster import AssetExecutionContext, MaterializeResult, RetryPolicy, asset
from pyspark import StorageLevel
from pyspark.sql import DataFrame

from ..resources import SparkResource, SqlServerResource
from ..transformations.redings_audits import (
    CONNECTION_LOST_THRESHOLD_MINUTES,
    LATE_READINGS_THRESHOLD_MINUTES,
    redings_audits,
)


INGESTION_NAME = "factRedingsAudits"
DEFAULT_FACTREDINGSAUDITS_OVERLAP_HOURS = 1
FACTREDINGSAUDITS_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)


@asset(
    name="observability_factredingsaudits",
    group_name="projectA",
    key_prefix=["projectA"],
    required_resource_keys={"spark", "sqlserver"},
    retry_policy=FACTREDINGSAUDITS_RETRY_POLICY,
)
def observability_factredingsaudits(
    context: AssetExecutionContext,
) -> MaterializeResult:
    start_time = perf_counter()
    spark_res: SparkResource = context.resources.spark
    sql: SqlServerResource = context.resources.sqlserver
    overlap_hours = int(
        os.getenv(
            "FACTREDINGSAUDITS_OVERLAP_HOURS",
            str(DEFAULT_FACTREDINGSAUDITS_OVERLAP_HOURS),
        )
    )
    if overlap_hours < 1:
        raise ValueError(
            "FACTREDINGSAUDITS_OVERLAP_HOURS must be greater than or equal to 1"
        )

    to_utc = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    last_loaded_at = sql.get_watermark(INGESTION_NAME)
    if last_loaded_at is not None and last_loaded_at.tzinfo is not None:
        last_loaded_at = (
            last_loaded_at
            .astimezone(timezone.utc)
            .replace(tzinfo=None, microsecond=0)
        )
    processing_anchor = min(last_loaded_at or to_utc, to_utc)
    from_utc = (
        processing_anchor - timedelta(hours=overlap_hours)
    ).replace(minute=0, second=0, microsecond=0)
    to_time_span = to_utc.replace(
        minute=0,
        second=0,
        microsecond=0,
    )

    sql.mark_run_started(INGESTION_NAME)
    run_id = context.run_id
    audit_df: DataFrame | None = None
    staged_rows = 0
    merged_rows = 0

    try:
        sql.clear_fact_redings_audits_stage(run_id)
        readings_df = spark_res.read_lecturas_range(from_utc, to_utc)
        dimensions_df = spark_res.read_dim_prod_dimensions()
        previous_health_df = spark_res.read_fact_redings_audits_state(
            from_utc,
            to_time_span,
        )
        audit_df = redings_audits(
            readings_df=readings_df,
            dimensions_df=dimensions_df,
            previous_health_df=previous_health_df,
            from_utc=from_utc,
            to_utc=to_time_span,
            as_of_utc=to_utc,
        )
        if audit_df is not None:
            audit_df = audit_df.persist(StorageLevel.MEMORY_AND_DISK)
            staged_rows = spark_res.write_fact_redings_audits_stage(
                audit_df,
                run_id,
            )
            merged_rows = sql.merge_fact_redings_audits(run_id)
        sql.clear_fact_redings_audits_stage(run_id)
        sql.mark_run_completed(
            ingestion_name=INGESTION_NAME,
            new_last_loaded_at=to_utc,
            succeeded=True,
            error_message=None,
        )
    except Exception as ex:
        try:
            sql.clear_fact_redings_audits_stage(run_id)
        except Exception:
            pass
        try:
            sql.mark_run_completed(
                ingestion_name=INGESTION_NAME,
                new_last_loaded_at=last_loaded_at,
                succeeded=False,
                error_message=str(ex),
            )
        except Exception:
            pass
        raise
    finally:
        if audit_df is not None:
            audit_df.unpersist()

    return MaterializeResult(
        metadata={
            "row_count": merged_rows,
            "staged_rows": staged_rows,
            "from_utc": str(from_utc),
            "to_utc": str(to_utc),
            "overlap_hours": overlap_hours,
            "late_threshold_minutes": LATE_READINGS_THRESHOLD_MINUTES,
            "connection_lost_threshold_minutes": (
                CONNECTION_LOST_THRESHOLD_MINUTES
            ),
            "elapsed_seconds": round(perf_counter() - start_time, 3),
        }
    )
