from dagster import AssetKey, AssetSelection, define_asset_job

fact_operations_tables_job = define_asset_job(
    name="fact_operations_tables_job",
    selection=AssetSelection.keys(
        AssetKey(["projectC", "data_transformations"]),
        AssetKey(["projectC", "dwh_factReadingsMeasurement"]),
        AssetKey(["projectC", "ctl_IngestionControl"])
    ),
    tags={"job_type": "operaciones", "user_code": "projectC"},
)
