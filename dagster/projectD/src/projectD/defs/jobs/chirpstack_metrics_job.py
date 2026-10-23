from dagster import AssetKey, AssetSelection, define_asset_job

chirpstack_metrics_job = define_asset_job(
    name="023_Sensor_chirpstack_metrics",
    selection=AssetSelection.keys(
        AssetKey(["projectD", "observability_chirpstackmetrics"]),
        AssetKey(["projectD", "observability_chirpstackmqttmetrics"]),
    ),
    tags={"job_type": "observability", "run_lane": "observability", "user_code_run_lane": "projectD_observability", "user_code": "projectD"},
)
