from dagster import AssetKey, AssetSelection, define_asset_job


telemetry_configuration_job = define_asset_job(
    name="telemetry_configuration_job",
    selection=AssetSelection.assets(AssetKey(["projectA", "telemetry_configuration"])),
    tags={"job_type": "alerting", "user_code_run_lane": "projectA_observability", "user_code": "projectA", "dagster/max_retries": "0"},
)
