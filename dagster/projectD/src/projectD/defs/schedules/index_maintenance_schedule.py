from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs.index_maintenance_job import index_maintenance_job


index_maintenance_schedule = ScheduleDefinition(
    name="081_maintenance_index_weekly",
    job=index_maintenance_job,
    cron_schedule=[
        "30 5 * * 0",
        "0 2 * * 3",
    ],
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
