from dagster import ScheduleDefinition, DefaultScheduleStatus
from ..jobs.cleanup_job import cleanup_job


cleanup_schedule = ScheduleDefinition(
    name="080_maintenance_cleanup_daily",
    job=cleanup_job,
    cron_schedule="0 0 * * 1-5",
    run_config={
        "ops": {
            "projectD__cleanup_old_runs": {
                "config": {"keep_days": 4, "batch_size": 10}
            }
        }
    },
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
