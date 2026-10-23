from dagster import AssetKey, AssetSelection, define_asset_job


observability_telemetry_user_usage_job = define_asset_job(
    name="121_observability_telemetry_user_usage",
    selection=AssetSelection.keys(
        AssetKey(["projectA", "observability_facttelemetryuserusage"]),
    ),
    tags={"job_type": "observability", "user_code_run_lane": "projectA_observability", "user_code": "projectA"},
)
