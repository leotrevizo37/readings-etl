from dagster import AssetKey, AssetSelection, define_asset_job


fact_sensor_events_job = define_asset_job(
    name="021_Sensor_sensor_events",
    selection=AssetSelection.keys(
        AssetKey(["projectA", "dwh_factsensorevents"]),
    ),
    tags={"job_type": "lecturas", "run_lane": "operational", "user_code_run_lane": "projectA_operational", "user_code": "projectA"},
)
