from dagster import AssetKey, AssetSelection, define_asset_job


telemetry_configuration_job = define_asset_job(
    name="telemetry_configuration_job",
    selection=AssetSelection.assets(AssetKey(["projectB", "telemetry_configuration"])),
    tags={"job_type": "alerting", "user_code_run_lane": "projectB_observability", "user_code": "projectB", "dagster/max_retries": "0"},
)
