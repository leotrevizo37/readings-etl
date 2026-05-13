from __future__ import annotations

from functools import lru_cache
from typing import Any

__all__ = ["defs"]


@lru_cache(maxsize=1)
def _build_defs() -> Any:
    from dagster import Definitions

    from .assets import (
        cleanup_old_runs,
        dbo_GroupHasSchedules,
        dbo_GroupHasSensors,
        dbo_GroupSensors,
        dbo_ProgramProfileTys,
        dbo_ProgramProfiles,
        dbo_ProgramSchedules,
        dbo_devices,
        dbo_sensor_tys,
        dbo_sensors,
        dbo_device_tys,
        dbo_sublocations,
        dbo_sublocation_tys,
        dwh_dimsensorexpectedval,
        dwh_dimsidonproddimensions,
        dwh_factlecturas,
        operations_assets,
    )
    from .jobs import (
        cleanup_job,
        fact_lecturas_ingestion_job,
        fact_operations_tables_job,
        refresh_dimensions_job,
        refresh_sensor_expected_values_job,
    )
    from .resources import (
        dim_sqlserver_resource,
        spark_df_io_manager,
        spark_resource,
        sqlserver_resource,
        transformation_ops,
    )
    from .schedules import (
        cleanup_schedule,
        fact_lecturas_sched_every_1h,
        fact_lecturas_sched_every_6h,
        fact_lecturas_sched_every_w,
        factlecturas_cleanup_schedule,
        operations_sched_every_6h,
        operations_sched_every_w,
        operations_sched_every_1h_w,
    )
    from .sensors import (
        dwh_dimsensorexpectedval_sensor,
        dwh_dimsidonproddimensions_sensor,
    )

    return Definitions(
        jobs=[
            fact_lecturas_ingestion_job,
            fact_operations_tables_job,
            refresh_dimensions_job,
            refresh_sensor_expected_values_job,
            cleanup_job,
        ],
        resources={
            "sqlserver": sqlserver_resource,
            "spark": spark_resource,
            "dim_sqlserver": dim_sqlserver_resource,
            "transformation_ops": transformation_ops,
            "spark_df_io_manager": spark_df_io_manager,
        },
        schedules=[
            fact_lecturas_sched_every_6h,
            fact_lecturas_sched_every_w,
            fact_lecturas_sched_every_1h,
            factlecturas_cleanup_schedule,
            operations_sched_every_1h_w,
            operations_sched_every_w,
            operations_sched_every_6h,
            cleanup_schedule,
        ],
        sensors=[
            dwh_dimsensorexpectedval_sensor,
            dwh_dimsidonproddimensions_sensor,
        ],
        assets=[
            dbo_sensors,
            dbo_sensor_tys,
            dbo_devices,
            dbo_device_tys,
            dbo_sublocations,
            dbo_sublocation_tys,
            dbo_ProgramProfileTys,
            dbo_ProgramProfiles,
            dbo_ProgramSchedules,
            dbo_GroupSensors,
            dbo_GroupHasSchedules,
            dbo_GroupHasSensors,
            dwh_factlecturas,
            operations_assets,
            dwh_dimsidonproddimensions,
            dwh_dimsensorexpectedval,
            cleanup_old_runs,
        ],
    )


def __getattr__(name: str) -> Any:
    if name == "defs":
        return _build_defs()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | {"defs"})
