from dagster import AssetKey, AssetSelection, define_asset_job

chirpstack_metrics_job = define_asset_job(
    name="023_Sensor_chirpstack_metrics",
    selection=AssetSelection.keys(
        AssetKey(["projectB", "observability_chirpstackmetrics"]),
        AssetKey(["projectB", "observability_chirpstackmqttmetrics"]),
    ),
    tags={"job_type": "observability", "run_lane": "observability", "user_code_run_lane": "projectB_observability", "user_code": "projectB"},
)
