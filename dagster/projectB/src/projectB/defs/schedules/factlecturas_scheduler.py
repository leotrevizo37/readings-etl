from dagster import ScheduleDefinition, DefaultScheduleStatus

from ..jobs import fact_lecturas_ingestion_job


fact_lecturas_sched_every_6h = ScheduleDefinition(
    name="010_Sensor_lecturas_ingestion_every_6h",
    job=fact_lecturas_ingestion_job,
    cron_schedule="0 */6 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

fact_lecturas_sched_every_1h = ScheduleDefinition(
    name="010_Sensor_lecturas_ingestion_every_1h",
    job=fact_lecturas_ingestion_job,
    cron_schedule="0 */1 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

fact_lecturas_sched_every_w = ScheduleDefinition(
    name="010_Sensor_lecturas_ingestion_every_w",
    job=fact_lecturas_ingestion_job,
    cron_schedule="0 21 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
