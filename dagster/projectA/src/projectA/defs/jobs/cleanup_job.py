from dagster import AssetKey, AssetSelection, define_asset_job

cleanup_job = define_asset_job(
    name="080_maintenance_cleanup",
    selection=AssetSelection.keys(
        AssetKey(["projectA", "cleanup_old_runs"]),
    ),
    tags={"job_type": "limpieza", "run_lane": "operational", "user_code_run_lane": "projectA_operational", "user_code": "projectA"},
)
