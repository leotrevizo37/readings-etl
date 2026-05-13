import os
from datetime import datetime, timedelta, timezone
from dagster import op, job, Output

@op(
    required_resource_keys={"sqlserver"},
)
def delete_old_factlecturas_op(context):
    sql = context.resources.sqlserver
    keep_days = int(os.getenv("FACTLECTURAS_KEEP_DAYS", "15"))
    
    to_utc = datetime.now(timezone.utc) - timedelta(days=keep_days)
    from_utc = datetime(2000, 1, 1, tzinfo=timezone.utc)
    
    deleted = sql.delete_range(
        schema_name="dwh",
        table_name="FactLecturas",
        time_column="TimeSpan",
        from_utc=from_utc,
        to_utc=to_utc
    )
    context.log.info(f"Deleted {deleted} rows older than {keep_days} days from dwh.FactLecturas.")
    
    return Output(
        value=deleted,
        metadata={
            "deleted_rows": deleted,
            "deleted_up_to": to_utc.isoformat()
        }
    )

@job(tags={"job_type": "limpieza", "user_code": "projectA"})
def factlecturas_cleanup_job():
    delete_old_factlecturas_op()
