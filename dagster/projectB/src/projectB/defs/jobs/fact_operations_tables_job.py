from dagster import AssetKey, AssetSelection, define_asset_job


fact_operations_tables_job = define_asset_job(
    name="020_Sensor_operations",
    selection=AssetSelection.keys(
        AssetKey(["projectB", "data_transformations"]),
        AssetKey(["projectB", "dwh_factReadingsMeasurement"]),
        AssetKey(["projectB", "ctl_IngestionControl"])
    ),
    tags={"job_type": "operaciones", "run_lane": "operational", "user_code_run_lane": "projectB_operational", "user_code": "projectB"},
)
