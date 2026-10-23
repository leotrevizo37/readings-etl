from dagster import ScheduleDefinition, DefaultScheduleStatus
from ..jobs.factlecturas_cleanup_job import factlecturas_cleanup_job


factlecturas_cleanup_schedule = ScheduleDefinition(
    name="011_Sensor_lecturas_cleanup_weekly",
    job=factlecturas_cleanup_job,
    cron_schedule="0 3 * * 0",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)
