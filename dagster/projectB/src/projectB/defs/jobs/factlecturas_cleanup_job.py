import os
from datetime import datetime, timedelta, timezone
from dagster import op, job, Output, RetryPolicy

FACTLECTURAS_CLEANUP_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)


@op(
    required_resource_keys={"sqlserver"},
    retry_policy=FACTLECTURAS_CLEANUP_RETRY_POLICY,
)
def delete_old_factlecturas_op(context):
    sql = context.resources.sqlserver
    keep_days = int(os.getenv("FACTLECTURAS_KEEP_DAYS", "15"))
    chunk_days = int(os.getenv("FACTLECTURAS_CLEANUP_CHUNK_DAYS", "1"))
    
    to_utc = datetime.now(timezone.utc) - timedelta(days=keep_days)
    from_utc = datetime(2000, 1, 1, tzinfo=timezone.utc)
    deleted = 0
    chunks = 0
    chunk_from = from_utc

    while chunk_from < to_utc:
        chunk_to = min(chunk_from + timedelta(days=chunk_days), to_utc)
        chunk_deleted = sql.delete_range(
            schema_name="dwh",
            table_name="FactLecturas",
            time_column="TimeSpan",
            from_utc=chunk_from,
            to_utc=chunk_to
        )
        deleted += chunk_deleted or 0
        chunks += 1
        chunk_from = chunk_to

    context.log.info(
        f"Deleted {deleted} rows older than {keep_days} days from "
        f"dwh.FactLecturas in {chunks} chunks."
    )
    
    return Output(
        value=deleted,
        metadata={
            "deleted_rows": deleted,
            "deleted_up_to": to_utc.isoformat(),
            "chunk_days": chunk_days,
            "chunks": chunks,
        }
    )

@job(name="011_Sensor_lecturas_cleanup", tags={"job_type": "limpieza", "run_lane": "operational", "user_code_run_lane": "projectB_operational", "user_code": "projectB"})
def factlecturas_cleanup_job():
    delete_old_factlecturas_op()
