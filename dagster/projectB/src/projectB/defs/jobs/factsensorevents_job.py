from dagster import AssetKey, AssetSelection, define_asset_job


fact_sensor_events_job = define_asset_job(
    name="021_Sensor_sensor_events",
    selection=AssetSelection.keys(
        AssetKey(["projectB", "dwh_factsensorevents"]),
    ),
    tags={"job_type": "lecturas", "run_lane": "operational", "user_code_run_lane": "projectB_operational", "user_code": "projectB"},
)
