from dagster import ScheduleDefinition, DefaultScheduleStatus
from ..jobs.cleanup_job import cleanup_job

cleanup_schedule = ScheduleDefinition(
    name="cleanup_schedule",
    job=cleanup_job,
    cron_schedule="0 0 * * 1-5",
    run_config={
        "ops": {
            "projectA__cleanup_old_runs": {
                "config": {"keep_days": 4, "batch_size": 10}
            }
        }
    },
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)