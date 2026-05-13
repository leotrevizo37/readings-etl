from dagster import AssetKey, AssetSelection, define_asset_job

cleanup_job = define_asset_job(
    name="cleanup_job",
    selection=AssetSelection.keys(
        AssetKey(["projectB", "cleanup_old_runs"]),
    ),
    tags={"job_type": "limpieza", "user_code": "projectB"},
)
