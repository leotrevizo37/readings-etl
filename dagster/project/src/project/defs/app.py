from dagster import Definitions

from .assets import (dbo_sensors,
                     dbo_sensor_tys,
                     dbo_devices,
                     dbo_device_tys,
                     dbo_sublocations,
                     dwh_readings,
                     operations_assets,
                     dwh_dimensions)
from .schedules import (readings_sched_every_2h,
                        operations_sched_every_4h)
from .jobs import (readings_ingestion_job,
                   fact_operations_tables_job,
                   refresh_dimensions_job)
from .sensors import dwh_dimensions_sensor

defs = Definitions(
    jobs=[
        readings_ingestion_job,
        fact_operations_tables_job,
        refresh_dimensions_job
    ],
    schedules=[
        readings_sched_every_2h,
        operations_sched_every_4h,
    ],
    sensors=[
        dwh_dimensions_sensor
    ],
    assets=[
        dbo_sensors,
        dbo_sensor_tys,
        dbo_devices,
        dbo_device_tys,
        dbo_sublocations,
        dwh_readings,
        operations_assets,
        dwh_dimensions
    ],
)
