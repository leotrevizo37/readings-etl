from dagster import AssetKey, AssetSelection, define_asset_job

fact_operations_tables_job = define_asset_job(
    name="020_Sensor_operations",
    selection=AssetSelection.keys(
        AssetKey(["projectA", "data_transformations"]),
        AssetKey(["projectA", "dwh_factReadingsMeasurement"]),
        AssetKey(["projectA", "ctl_IngestionControl"])
    ),
    tags={"job_type": "operaciones", "run_lane": "operational", "user_code_run_lane": "projectA_operational", "user_code": "projectA"},
)
