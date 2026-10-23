from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from ..jobs.observability_dim_aspnet_users_job import (
    observability_dim_aspnet_users_job,
)


@sensor(
    job=observability_dim_aspnet_users_job,
    minimum_interval_seconds=3600,
    required_resource_keys={"dim_sqlserver"},
    name="observability_dimaspnetusers_sensor",
    default_status=DefaultSensorStatus.RUNNING,
)
def observability_dimaspnetusers_sensor(
    context: SensorEvaluationContext,
):
    dim_sql = context.resources.dim_sqlserver

    try:
        current_wm = dim_sql._get_max_modified_at(["AspNetUsers"])
    except Exception as ex:
        context.log.warning(
            "Skipping AspNetUsers refresh sensor because watermark lookup failed: %s",
            ex,
        )
        return SkipReason(
            "AspNetUsers source watermark is currently unavailable."
        )

    current_wm_str = current_wm.isoformat() if current_wm else "NULL"
    previous_cursor = context.cursor

    if previous_cursor == current_wm_str:
        return SkipReason(
            f"No AspNetUsers changes detected. watermark={current_wm_str}"
        )

    context.update_cursor(current_wm_str)
    return RunRequest(
        run_key=(
            f"bootstrap:{current_wm_str}"
            if previous_cursor is None
            else current_wm_str
        ),
        tags={
            "dims_asset": "observability_dimaspnetusers",
            "source_watermark": current_wm_str,
        },
    )

