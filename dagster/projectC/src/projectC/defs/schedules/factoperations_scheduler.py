from dagster import ScheduleDefinition, DefaultScheduleStatus

from ..jobs import fact_operations_tables_job


operations_sched_every_6h = ScheduleDefinition(
    name="operations_sched_every_6h",
    job=fact_operations_tables_job,
    cron_schedule="30 */6 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

operations_sched_every_w = ScheduleDefinition(
    name="operations_sched_every_w",
    job=fact_operations_tables_job,
    cron_schedule="30 5-16/3 * * 1-5",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

operations_sched_every_1h_w = ScheduleDefinition(
    name="operations_sched_every_1h_w",
    job=fact_operations_tables_job,
    cron_schedule="30 5-16 * * 1-5",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
