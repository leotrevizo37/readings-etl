from dagster import ScheduleDefinition, DefaultScheduleStatus

from ..jobs import fact_sensor_events_job


sensor_events_sched_every_6h = ScheduleDefinition(
    name="021_Sensor_sensor_events_every_6h",
    job=fact_sensor_events_job,
    cron_schedule=[
        "40 */6 * * *",
        "0 9,16 * * *",
    ],
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

sensor_events_sched_every_1h_w = ScheduleDefinition(
    name="021_Sensor_sensor_events_every_1h_w",
    job=fact_sensor_events_job,
    cron_schedule="40 5-16 * * 1-5",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)
