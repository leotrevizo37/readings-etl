from dagster import AssetKey, AssetSelection, define_asset_job

fact_operations_tables_job = define_asset_job(
    name="fact_operations_tables_job",
    selection=AssetSelection.keys(
        AssetKey(["projectA", "data_transformations"]),
        AssetKey(["projectA", "dwh_factReadingsMeasurement"]),
        AssetKey(["projectA", "ctl_IngestionControl"])
    ),
    tags={"job_type": "operaciones", "user_code": "projectA"},
)
