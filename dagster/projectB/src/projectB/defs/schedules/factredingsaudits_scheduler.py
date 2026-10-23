from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs import fact_redings_audits_job


redings_audits_sched_every_20m = ScheduleDefinition(
    name="022_observability_redings_audits_every_20m",
    job=fact_redings_audits_job,
    cron_schedule="*/20 * * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
