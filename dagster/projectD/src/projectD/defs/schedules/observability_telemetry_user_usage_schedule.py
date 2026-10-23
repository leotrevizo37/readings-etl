from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs.observability_telemetry_user_usage_job import (
    observability_telemetry_user_usage_job,
)


observability_telemetry_user_usage_daily = ScheduleDefinition(
    name="121_observability_telemetry_user_usage_daily",
    job=observability_telemetry_user_usage_job,
    cron_schedule="30 1 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)

