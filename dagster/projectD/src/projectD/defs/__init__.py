from __future__ import annotations

from functools import lru_cache
from typing import Any

__all__ = ["defs"]


@lru_cache(maxsize=1)
def _build_defs() -> Any:
    from dagster import Definitions

    from .asset_checks import quality_asset_checks
    from .assets import (
        data_transformations,
        telemetry_configuration,
        cleanup_old_runs,
        dbo_devices,
        dbo_sensor_tys,
        dbo_sensors,
        dbo_device_tys,
        dbo_sublocations,
        dbo_sublocation_tys,
        dwh_dimtelemetryproddimensions,
        dwh_factlecturas,
        dwh_factsensorevents,
        observability_factredingsaudits,
        observability_chirpstackmetrics,
        observability_chirpstackmqttmetrics,
        operations_assets,
    )
    from .jobs import (
        telemetry_configuration_job,
        cleanup_job,
        fact_lecturas_backfill_daily_job,
        fact_lecturas_ingestion_job,
        fact_lecturas_sensor_backfill_job,
        fact_operations_tables_job,
        fact_redings_audits_job,
        fact_sensor_events_job,
        chirpstack_metrics_job,
        index_maintenance_job,
        refresh_dimensions_job,
        spark_df_io_manager_healthcheck_job,
    )
    from .resources import (
        telemetry_configuration_ai_resource,
        dim_sqlserver_resource,
        chirpstack_api_resource,
        mqtt_event_history_api_resource,
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
        index_maintenance_schedule,
        operations_sched_every_6h,
        operations_sched_every_w,
        operations_sched_every_1h_w,
        redings_audits_sched_every_10m,
        redings_audits_sched_every_40m,
        sensor_events_sched_every_6h,
        sensor_events_sched_every_1h_w,
        chirpstack_metrics_sched_hourly_at_30,
    )
    from .sensors import (
        telemetry_configuration_sensor,
        asset_row_count_trend_sensor,
        dwh_dimtelemetryproddimensions_sensor,
        operational_alerts_sensor,
    )

    return Definitions(
        jobs=[
            telemetry_configuration_job,
            fact_lecturas_ingestion_job,
            fact_lecturas_backfill_daily_job,
            fact_lecturas_sensor_backfill_job,
            fact_operations_tables_job,
            fact_redings_audits_job,
            fact_sensor_events_job,
            chirpstack_metrics_job,
            refresh_dimensions_job,
            spark_df_io_manager_healthcheck_job,
            index_maintenance_job,
            cleanup_job,
        ],
        resources={
            "telemetry_configuration_ai": telemetry_configuration_ai_resource,
            "sqlserver": sqlserver_resource,
            "chirpstack_api": chirpstack_api_resource,
            "mqtt_event_history_api": mqtt_event_history_api_resource,
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
            redings_audits_sched_every_10m,
            redings_audits_sched_every_40m,
            sensor_events_sched_every_6h,
            sensor_events_sched_every_1h_w,
            chirpstack_metrics_sched_hourly_at_30,
            cleanup_schedule,
            index_maintenance_schedule,
        ],
        sensors=[
            telemetry_configuration_sensor,
            asset_row_count_trend_sensor,
            dwh_dimtelemetryproddimensions_sensor,
            operational_alerts_sensor,
        ],
        assets=[
            data_transformations,
            telemetry_configuration,
            dbo_sensors,
            dbo_sensor_tys,
            dbo_devices,
            dbo_device_tys,
            dbo_sublocations,
            dbo_sublocation_tys,
            dwh_factlecturas,
            dwh_factsensorevents,
            observability_factredingsaudits,
            observability_chirpstackmetrics,
            observability_chirpstackmqttmetrics,
            operations_assets,
            dwh_dimtelemetryproddimensions,
            cleanup_old_runs,
        ],
        asset_checks=quality_asset_checks,
    )


def __getattr__(name: str) -> Any:
    if name == "defs":
        return _build_defs()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | {"defs"})
