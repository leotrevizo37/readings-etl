from dagster import AssetKey, AssetSelection, define_asset_job


telemetry_configuration_job = define_asset_job(
    name="telemetry_configuration_job",
    selection=AssetSelection.assets(AssetKey(["projectD", "telemetry_configuration"])),
    tags={"job_type": "alerting", "user_code_run_lane": "projectD_observability", "user_code": "projectD", "dagster/max_retries": "0"},
)
