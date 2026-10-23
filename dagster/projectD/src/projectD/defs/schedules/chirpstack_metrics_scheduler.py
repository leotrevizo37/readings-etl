from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs import chirpstack_metrics_job

chirpstack_metrics_sched_hourly_at_30 = ScheduleDefinition(
    name="023_Sensor_chirpstack_metrics_hourly_at_30",
    job=chirpstack_metrics_job,
    cron_schedule="30 * * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
