from dagster import AssetKey, AssetSelection, define_asset_job


fact_lecturas_ingestion_job = define_asset_job(
    name="010_Sensor_lecturas_ingestion",
    selection=AssetSelection.keys(AssetKey(["projectD", "dwh_factlecturas"])),
    tags={"job_type": "lecturas", "run_lane": "operational", "user_code_run_lane": "projectD_operational", "user_code": "projectD"},
)
