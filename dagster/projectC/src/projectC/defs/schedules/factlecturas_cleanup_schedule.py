from dagster import ScheduleDefinition, DefaultScheduleStatus
from ..jobs.factlecturas_cleanup_job import factlecturas_cleanup_job

factlecturas_cleanup_schedule = ScheduleDefinition(
    name="factlecturas_cleanup_schedule",
    job=factlecturas_cleanup_job,
    cron_schedule="0 3 * * 0",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
