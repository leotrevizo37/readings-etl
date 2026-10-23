from dagster import ScheduleDefinition, DefaultScheduleStatus

from ..jobs import fact_audiencia_ingestion_job


fact_audiencia_sched_every_6h = ScheduleDefinition(
    name="014_Sensor_audiencia_ingestion_every_6h",
    job=fact_audiencia_ingestion_job,
    cron_schedule="0 */6 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

fact_audiencia_sched_every_1h = ScheduleDefinition(
    name="014_Sensor_audiencia_ingestion_every_1h",
    job=fact_audiencia_ingestion_job,
    cron_schedule="0 */1 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

fact_audiencia_sched_every_w = ScheduleDefinition(
    name="014_Sensor_audiencia_ingestion_every_w",
    job=fact_audiencia_ingestion_job,
    cron_schedule=[
        "0 7-17/2 * * 1-5",
        "0 23 * * 1-5",
        "0 11,23 * * 0,6",
    ],
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
