from dagster import ScheduleDefinition, DefaultScheduleStatus

from ..jobs import fact_operations_tables_job

operations_sched_every_4h = ScheduleDefinition(
    name="operations_sched_every_4h",
    job=fact_operations_tables_job,
    cron_schedule="30 */4 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,

)

operations_sched_every_6h = ScheduleDefinition(
    name="operations_sched_every_6h",
    job=fact_operations_tables_job,
    cron_schedule="30 */6 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,

)

operations_sched_every_2h = ScheduleDefinition(
    name="operations_sched_every_2h",
    job=fact_operations_tables_job,
    cron_schedule="20 */2 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,

)
