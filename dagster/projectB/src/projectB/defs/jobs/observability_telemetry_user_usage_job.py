from dagster import AssetKey, AssetSelection, define_asset_job


observability_telemetry_user_usage_job = define_asset_job(
    name="121_observability_telemetry_user_usage",
    selection=AssetSelection.keys(
        AssetKey(["projectB", "observability_facttelemetryuserusage"]),
    ),
    tags={"job_type": "observability", "user_code_run_lane": "projectB_observability", "user_code": "projectB"},
)

