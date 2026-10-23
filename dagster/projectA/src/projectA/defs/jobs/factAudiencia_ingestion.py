from dagster import AssetKey, AssetSelection, define_asset_job

fact_audiencia_ingestion_job = define_asset_job(
    name="014_Sensor_audiencia_ingestion",
    selection=AssetSelection.keys(AssetKey(["projectA", "dwh_factaudiencia"])),
    tags={"job_type": "lecturas", "run_lane": "operational", "user_code_run_lane": "projectA_operational", "user_code": "projectA"},
)
