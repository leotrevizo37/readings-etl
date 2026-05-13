from __future__ import annotations

from dagster import (RunRequest,
                     SensorEvaluationContext,
                     SkipReason,
                     sensor,
                     DefaultSensorStatus)

from ..jobs import refresh_sensor_expected_values_job


@sensor(
    job=refresh_sensor_expected_values_job,
    minimum_interval_seconds=3600,
    required_resource_keys={"dim_sqlserver"},
    name="dwh_dimsensorexpectedval_sensor",
    default_status=DefaultSensorStatus.RUNNING
)
def dwh_dimsensorexpectedval_sensor(context: SensorEvaluationContext):
    dim_sql = context.resources.dim_sqlserver

    try:
        current_wm = dim_sql.get_watermark_sensor_expected_values()
    except Exception as ex:
        context.log.warning(
            "Skipping dimSensorExpectedVal refresh sensor because watermark lookup failed: %s",
            ex,
        )
        return SkipReason("dimSensorExpectedVal source watermark is currently unavailable.")

    current_wm_str = current_wm.isoformat() if current_wm else "NULL"

    prev_cursor = context.cursor

    if prev_cursor is None:
        return RunRequest(
            run_key=f"bootstrap:{current_wm_str}",
            tags={"dims_asset": "dwh_dimsensorexpectedval", "source_watermark": current_wm_str},
        )

    if prev_cursor == current_wm_str:
        return SkipReason(f"No changes detected. watermark={current_wm_str}")

    return RunRequest(
        run_key=current_wm_str,
        tags={
            "dims_asset": "dwh_dimsensorexpectedval",
            "source_watermark": current_wm_str,
        },
        cursor=current_wm_str,
    )
