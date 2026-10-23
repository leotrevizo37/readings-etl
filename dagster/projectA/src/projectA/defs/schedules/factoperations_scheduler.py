from dagster import ScheduleDefinition, DefaultScheduleStatus

from ..jobs import fact_operations_tables_job


operations_sched_every_6h = ScheduleDefinition(
    name="020_Sensor_operations_every_6h",
    job=fact_operations_tables_job,
    cron_schedule="0 */6 * * 0,6",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)

operations_sched_every_w = ScheduleDefinition(
    name="020_Sensor_operations_every_w",
    job=fact_operations_tables_job,
    cron_schedule="0 1,5,21 * * 1-5",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)

operations_sched_every_1h_w = ScheduleDefinition(
    name="020_Sensor_operations_every_1h_w",
    job=fact_operations_tables_job,
    cron_schedule=[
        "0 8-18/2 * * 1-5",
        "40 8-16/2 * * 1-5",
        "20 9-17/2 * * 1-5",
    ],
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
