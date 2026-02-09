from dagster import ScheduleDefinition, DefaultScheduleStatus

from ..jobs import readings_ingestion_job

readings_sched_every_3h = ScheduleDefinition(
    name="ingest_readings_every_3h",
    job=readings_ingestion_job,
    cron_schedule="0 */3 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
)

readings_sched_every_2h = ScheduleDefinition(
    name="ingest_readings_every_2h",
    job=readings_ingestion_job,
    cron_schedule="0 */2 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)

readings_sched_every_6h = ScheduleDefinition(
    name="ingest_readings_every_6h",
    job=readings_ingestion_job,
    cron_schedule="0 */3 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
)

readings_sched_every_1h = ScheduleDefinition(
    name="ingest_readings_every_1h",
    job=readings_ingestion_job,
    cron_schedule="0 */1 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)