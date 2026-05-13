from dagster import AssetKey, AssetSelection, define_asset_job

fact_operations_tables_job = define_asset_job(
    name="fact_operations_tables_job",
    selection=AssetSelection.keys(
        AssetKey(["projectB", "data_transformations"]),
        AssetKey(["projectB", "dwh_factReadingsMeasurement"]),
        AssetKey(["projectB", "ctl_IngestionControl"])
    ),
    tags={"job_type": "operaciones", "user_code": "projectB"},
)
