from dagster import AssetKey, AssetSelection, define_asset_job


fact_redings_audits_job = define_asset_job(
    name="022_Sensor_redings_audits",
    selection=AssetSelection.keys(
        AssetKey(["projectD", "observability_factredingsaudits"]),
    ),
    tags={"job_type": "observability", "run_lane": "observability", "user_code_run_lane": "projectD_observability", "user_code": "projectD"},
)
