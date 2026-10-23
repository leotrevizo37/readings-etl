import os

from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs.observability_url_availability_job import (
    observability_url_availability_job,
)


observability_url_availability_schedule = ScheduleDefinition(
    name="122_observability_url_availability_schedule",
    job=observability_url_availability_job,
    cron_schedule=os.getenv(
        "PROMETHEUS_ENDPOINT_PROBE_CRON",
        "0 */2 * * *",
    ),
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)

